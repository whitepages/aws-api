;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.client
  "Impl, don't call directly."
  (:require [clojure.core.async :as a]
            [byte-streams :as byte-streams]
            [cognitect.aws.http :as http]
            [cognitect.aws.util :as util]
            [cognitect.aws.interceptors :as interceptors]
            [cognitect.aws.endpoint :as endpoint]
            [cognitect.aws.region :as region]
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
  (fn [service endpoint credentials http-request]
    (get-in service [:metadata :signatureVersion])))

;; TODO convey throwable back from impl
(defn ^:private handle-http-response
  [service op-map http-response]
  (try
    (if-let [anomaly-category (:cognitect.anomaly/category http-response)]
      {:cognitect.anomalies/category anomaly-category
       ::throwable                   (:cognitect.http-client/throwable http-response)}
      (parse-http-response service op-map http-response))
    (catch Throwable t
      {:cognitect.anomalies/category :cognitect.anomalies/fault
       ::throwable                   t})))

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
  "Creates a Ring request map that needs to be signed before being sent by `cognitect.http-client/submit`."
  [client op-map]
  (let [{:keys [service endpoint]} (-get-info client)]
    (-> (build-http-request service op-map)
        (with-endpoint endpoint)
        (update :body #(some-> % byte-streams/to-byte-buffer))
        ((partial interceptors/modify-http-request service op-map)))))

(defn sign-http-request-with-client
  "Signs a request with a client and a request returned by `http-request`."
  [client request]
  (let [{:keys [service region credentials]} (-get-info client)]
    (sign-http-request service region (credentials/fetch credentials) request)))

(defn send-request
  "For internal use. Send the request to AWS and return a channel which delivers the response.

  Alpha. Subject to change."
  [client {:keys [with-meta?]
           :or {with-meta? true}
           :as op-map}]
  (let [{:keys [service http-client region-provider credentials-provider endpoint-provider] :as client-info}
        (-get-info client)
        response-meta (atom {})
        region-ch     (region/fetch-async region-provider)
        creds-ch      (credentials/fetch-async credentials-provider)
        result-ch     (a/promise-chan (map #(cond-> (handle-http-response service op-map %)
                                              with-meta? (with-meta (assoc @response-meta :http-response (dissoc % :body))))))
        send-http     (or (:send-http op-map)
                          (:send-http client-info))]

    (a/go
      (let [region   (a/<! region-ch)
            creds    (a/<! creds-ch)
            endpoint (endpoint/fetch endpoint-provider region)]
        (cond
          (:cognitect.anomalies/category region)
          (a/>! result-ch region)
          (:cognitect.anomalies/category creds)
          (a/>! result-ch creds)
          (:cognitect.anomalies/category endpoint)
          (a/>! result-ch endpoint)
          :else
          (try
            (let [http-request (http-request client op-map)]
              (swap! response-meta assoc :http-request http-request)
              (send-http http-request http-client op-map result-ch)
              result-ch)
            (catch Throwable t
              (let [err-ch (a/promise-chan)]
                (a/put! err-ch (cond-> {:cognitect.anomalies/category :cognitect.anomalies/fault
                                        ::throwable                   t}
                                 with-meta? (with-meta (swap! response-meta assoc :op-map op-map))))
                err-ch))))))))
