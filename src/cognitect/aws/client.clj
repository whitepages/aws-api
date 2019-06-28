;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.client
  "Impl, don't call directly."
  (:require [clojure.core.async :as a]
            [byte-streams :as byte-streams]
            [cognitect.aws
             [credentials :as credentials]
             [interceptors :as interceptors]]))

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
  "Send the request to AWS and return a channel which delivers the response."
  [client {:keys [with-meta?]
           :or   {with-meta? true}
           :as   op-map}]
  (let [{:keys [service send-http]} (-get-info client)
        result-meta                 (atom {})]
    (try
      (let [req         (http-request client op-map)
            result-chan (a/chan 1 (map #(cond-> (handle-http-response service op-map %)
                                          with-meta? (with-meta (assoc @result-meta :http-response (dissoc % :body))))))]
        (swap! result-meta assoc :http-request req)
        (send-http req client op-map result-chan)
        result-chan)
      (catch Throwable t
        (let [err-ch (a/chan 1)]
          (a/put! err-ch (cond-> {:cognitect.anomalies/category :cognitect.anomalies/fault
                                  ::throwable                   t}
                           with-meta? (with-meta @result-meta)))
          err-ch)))))
