;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns cognitect.aws.client.api
  "API functions for using a client to interact with AWS services."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [cognitect.aws.client :as client]
            [cognitect.aws.retry :as retry]
            [cognitect.aws.credentials :as credentials]
            [cognitect.aws.endpoint :as endpoint]
            [cognitect.aws.http :as http]
            [cognitect.aws.service :as service]
            [cognitect.aws.region :as region]
            [cognitect.aws.client.api.async :as api.async]
            [cognitect.aws.signers] ;; implements multimethods
            [cognitect.aws.util :as util]))

(declare ops sign-http-request)

;; TODO: extract this to a protocol
(defn default-http-send [http-client]
  (fn send-http
    ([req client op-map]
     (send-http req client op-map (async/promise-chan)))
    ([req client op-map chan]
     (http/submit
      http-client
      (merge (cond->> req
               client (sign-http-request client))
             (select-keys op-map [:cognitect.http-client/timeout-msec]))
      chan))))

(defn client
  "Given a config map, create a client for specified api. Supported keys:

  :api                  - required, this or api-descriptor required, the name of the api
                          you want to interact with e.g. :s3, :cloudformation, etc
  :region               - optional, the aws region serving the API endpoints you
                          want to interact with, defaults to region provided by
                          by the default region provider (see `cognitect.aws.region`)
  :credentials-provider - optional, implementation of
                          `cognitect.aws.credentials/CredentialsProvider`
                          protocol, defaults to
                          `cognitect.aws.credentials/default-credentials-provider`
  :endpoint-override    - optional, map to override parts of the endpoint. Supported keys:
                            :protocol     - :http or :https
                            :hostname     - string
                            :port         - int
                            :path         - string
                          If the hostname includes an AWS region, be sure use the same
                          region for the client (either via out of process configuration
                          or the :region key supplied to this fn).
                          Also supports a string representing just the hostname, though
                          support for a string is deprectated and may be removed in the
                          future.
  :send-http            - optional, a user supplied function which takes a cognitect-http
                          flavored request map, the client, the op-map from `invoke`
                          and an optional arg for a core.async channel where the function should put! the result on.
                          Since the request can be modified before being sent with a client,
                          this function is responsible for signing the request with `sign-http-request`.
                          The return value must be the a channel returning the result, if a channel was passed
                          in it should be returned. Default impl found in `default-http-send`.
  :http-client          - optional, an `cognitect.http-client/Client` implementation
  :region-provider      - optional, implementation of `aws-clojure.region/RegionProvider`
                          protocol, defaults to `cognitect.aws.region/default-region-provider`
  :retriable?           - optional, fn of http-response (see `cognitect.http-client/submit`).
                          Should return a boolean telling the client whether or
                          not the request is retriable.  The default,
                          `cognitect.aws.retry/default-retriable?`, returns
                          true when the response indicates that the service is
                          busy or unavailable.
  :backoff              - optional, fn of number of retries so far. Should return
                          number of milliseconds to wait before the next retry
                          (if the request is retriable?), or nil if it should stop.
                          Defaults to `cognitect.aws.retry/default-backoff`.

  Alpha. Subject to change."
  [{:keys [api region region-provider retriable? backoff http-client send-http credentials-provider endpoint-override]
    :or   {endpoint-override {}}
    :as config}]
  (when (string? endpoint-override)
    (log/warn
     (format
      "DEPRECATION NOTICE: :endpoint-override string is deprecated.\nUse {:endpoint-override {:hostname \"%s\"}} instead."
      endpoint-override)))
  (let [service     (service/service-description (name api))
        http-client (cond
                      http-client     http-client
                      (not send-http) (http/resolve-http-client http-client))
        region      (keyword
                     (or region
                         (region/fetch
                          (or region-provider
                              (region/default-region-provider http-client)))))]
    (require (symbol (str "cognitect.aws.protocols." (get-in service [:metadata :protocol]))))
    (with-meta
      (client/->Client
       (atom {})
       {:service     service
        :region      region
        :endpoint    (if-let [ep (endpoint/resolve (-> service :metadata :endpointPrefix keyword) region)]
                       (merge ep (if (string? endpoint-override)
                                   {:hostname endpoint-override}
                                   endpoint-override))
                       (throw (ex-info "No known endpoint." {:service api :region region})))
        :retriable?  (or retriable? retry/default-retriable?)
        :backoff     (or backoff retry/default-backoff)
        :send-http   send-http
        :http-client http-client
        :credentials (or credentials-provider (credentials/global-provider (or send-http http-client)))})
      {'clojure.core.protocols/datafy (fn [c]
                                        (-> c
                                            client/-get-info
                                            (select-keys [:region :endpoint :service :send-http])
                                            (update :endpoint select-keys [:hostname :protocols :signatureVersions])
                                            (update :service select-keys [:metadata])
                                            (assoc :ops (ops c))))})))

(defn default-http-client
  "Create an http-client to share across multiple aws-api clients."
  []
  (http/resolve-http-client nil))

(defn invoke
  "Package and send a request to AWS and return the result.

  Supported keys in op-map:

  :op                   - required, keyword, the op to perform
  :request              - required only for ops that require them.
  :retriable?           - optional, defaults to :retriable? on the client.
                          See client.
  :backoff              - optional, defaults to :backoff on the client.
                          See client.

  After invoking (cognitect.aws.client.api/validate-requests true), validates
  :request in op-map.

  Alpha. Subject to change."
  [client op-map]
  (async/<!! (api.async/invoke client op-map)))

(defn http-request
  "Returns a request map that once signed via `sign-http-request`
  can be sent to AWS with `cognitect.http-client/submit`.

  Alpha. Subject to change."
  [client op-map]
  (client/http-request client op-map))

(defn sign-http-request
  "Signs request so it can be sent to

  Alpha. Subject to change."
  [client request]
  (client/sign-http-request-with-client client request))

(defn validate-requests
  "Given true, uses clojure.spec to validate all invoke calls on client.

  Alpha. Subject to change."
  ([client]
   (validate-requests client true))
  ([client bool]
   (api.async/validate-requests client bool)))

(defn request-spec-key
  "Returns the key for the request spec for op.

  Alpha. Subject to change."
  [client op]
  (service/request-spec-key (-> client client/-get-info :service) op))

(defn response-spec-key
  "Returns the key for the response spec for op.

  Alpha. Subject to change."
  [client op]
  (service/response-spec-key (-> client client/-get-info :service) op))

(def ^:private pprint-ref (delay (util/dynaload 'clojure.pprint/pprint)))
(defn ^:skip-wiki pprint
  "For internal use. Don't call directly."
  [& args]
  (binding [*print-namespace-maps* false]
    (apply @pprint-ref args)))

(defn ops
  "Returns a map of operation name to operation data for this client.

  Alpha. Subject to change."
  [client]
  (-> client
      client/-get-info
      :service
      service/docs))

(defn doc-str
  "Given data produced by `ops`, returns a string
  representation.

  Alpha. Subject to change."
  [{:keys [documentation request required response refs] :as doc}]
  (when doc
    (str/join "\n"
              (cond-> ["-------------------------"
                       (:name doc)
                       ""
                       documentation]
                request
                (into [""
                       "-------------------------"
                       "Request"
                       ""
                       (with-out-str (pprint request))])
                required
                (into ["Required"
                       ""
                       (with-out-str (pprint required))])
                response
                (into ["-------------------------"
                       "Response"
                       ""
                       (with-out-str (pprint response))])
                refs
                (into ["-------------------------"
                       "Given"
                       ""
                       (with-out-str (pprint refs))])))))

(defn doc
  "Given a client and an operation (keyword), prints documentation
  for that operation to the current value of *out*. Returns nil.

  Alpha. Subject to change."
  [client operation]
  (println (or (some-> client ops operation doc-str)
               (str "No docs for " (name operation)))))

(defn stop
  "Shuts down the underlying http-client, releasing resources.

  NOTE: if you're sharing an http-client across aws-api clients,
  this will shut down the shared client for all aws-api clients
  that are using it.

  Alpha. Subject to change."
  [client]
  (some-> client
          client/-get-info
          :http-client
          http/stop))
