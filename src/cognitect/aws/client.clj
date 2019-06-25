;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.client
  "Impl, don't call directly."
  (:require [clojure.core.async :as a]
            [cognitect.aws.util :as util]
            [cognitect.aws.interceptors :as interceptors]
            [cognitect.aws.credentials :as credentials]))

(set! *warn-on-reflection* true)

(defprotocol ClientSPI
  (-get-info [_] "Intended for internal use only"))

(deftype Client [client-meta info]
  clojure.lang.IObj
  (meta [_] @client-meta)
  (withMeta [this m] (swap! client-meta merge m) this)

  ClientSPI
  (-get-info [_] info))

(defmulti build-http-request
  "AWS request -> HTTP request."
  (fn [service op-map]
    (get-in service [:metadata :protocol])))

(defmulti parse-http-response
  "HTTP response -> AWS response"
  (fn [service op-map http-response]
    (get-in service [:metadata :protocol])))

(defmulti sign-http-request
  "Sign the HTTP request."
  (fn [service region credentials http-request]
    (get-in service [:metadata :signatureVersion])))

(defn ^:private handle-http-response
  [service op-map http-response]
  (try
    (if-let [anomaly-category (:cognitect.anomaly/category http-response)]
      {:cognitect.anomalies/category anomaly-category
       ::throwable (:cognitect.http-client/throwable http-response)}
      (parse-http-response service op-map http-response))
    (catch Throwable t
      {:cognitect.anomalies/category :cognitect.anomalies/fault
       ::throwable t})))

(defn ^:private with-endpoint [req {:keys [protocol
                                           hostname
                                           port
                                           path]
                                    :as   endpoint}]
  (cond-> (-> req
              (assoc-in [:headers "host"] hostname)
              (assoc :server-name hostname))
    protocol (assoc :scheme protocol)
    port     (assoc :server-port port)
    path     (assoc :uri path)))

(defn http-request
  "Creates a Ring request map ready to be sent via any Ring-compatible HTTP client."
  [client op-map]
  (let [{:keys [service region credentials endpoint]} (-get-info client)]
    (sign-http-request service region (credentials/fetch credentials)
                       (-> (build-http-request service op-map)
                           (with-endpoint endpoint)
                           (update :body util/->bbuf)
                           ((partial interceptors/modify-http-request service op-map))))))

(defn send-request
  "Send the request to AWS and return a channel which delivers the response."
  [client op-map]
  (let [{:keys [service send-http]} (-get-info client)
        result-meta                 (atom {})]
    (try
      (let [req         (http-request client op-map)
            result-chan (a/chan 1 (map #(with-meta
                                          (handle-http-response service op-map %)
                                          (assoc @result-meta
                                                 :http-response
                                                 (update % :body util/bbuf->input-stream)))))]
        (swap! result-meta assoc :http-request req)
        (send-http req op-map result-chan))
      (catch Throwable t
        (let [err-ch (a/chan 1)]
          (a/put! err-ch (with-meta
                           {:cognitect.anomalies/category :cognitect.anomalies/fault
                            ::throwable                   t}
                           @result-meta))
          err-ch)))))
