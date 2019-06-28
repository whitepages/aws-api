;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.ec2-metadata-utils
  "Impl, don't call directly"
  (:require [clojure.string :as str]
            [byte-streams :as byte-streams]
            [clojure.core.async :as a]
            [cognitect.aws.util :as u]
            [cognitect.aws.retry :as retry])
  (:import (java.net URI)))

(def ^:const ec2-metadata-service-override-system-property "com.amazonaws.sdk.ec2MetadataServiceEndpointOverride")
(def ^:const dynamic-data-root "/latest/dynamic/")
(def ^:const security-credentials-path "/latest/meta-data/iam/security-credentials/")
(def ^:const instance-identity-document "instance-identity/document")
(def ^:const allowed-hosts #{"127.0.0.1" "localhost"})

;; ECS
(def ^:const container-credentials-relative-uri-env-var "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
(def ^:const container-credentials-full-uri-env-var "AWS_CONTAINER_CREDENTIALS_FULL_URI")

(def ^:const ec2-metadata-host "http://169.254.169.254")
(def ^:const ecs-metadata-host "http://169.254.170.2")

(defn in-container? []
  (or (u/getenv container-credentials-relative-uri-env-var)
      (u/getenv container-credentials-full-uri-env-var)))

(defn build-path [& components]
  (str/replace (str/join \/ components) #"\/\/+" (constantly "/")))

(defn- build-uri
  [host path]
  (str host "/" (cond-> path (str/starts-with? path "/") (subs 1))))

(defn get-host-address
  "Gets the EC2 (or ECS) metadata host address"
  []
  (or (u/getProperty ec2-metadata-service-override-system-property)
      (when (in-container?) ecs-metadata-host)
      ec2-metadata-host))

(defn- request-map
  [^URI uri]
  {:scheme         (.getScheme uri)
   :server-name    (.getHost uri)
   :server-port    (or (when (pos? (.getPort uri))
                         (.getPort uri))
                       (when (= (.getScheme uri) :https)
                         443)
                       80)
   :uri            (.getPath uri)
   :request-method :get
   :headers        {:accept "*/*"}})

(defn get-data [uri send-http]
  (let [response (a/<!! (retry/with-retry
                          #(send-http (request-map (URI. uri)) nil nil)
                          (a/promise-chan)
                          retry/default-retriable?
                          retry/default-backoff))]
    ;; TODO: handle unhappy paths -JS
    (when (= 200 (:status response))
      (-> response
          :body
          byte-streams/to-string))))

(defn get-data-at-path [path send-http]
  (get-data (build-uri (get-host-address) path) send-http ))

(defn get-listing [uri send-http]
  (some-> (get-data uri send-http) str/split-lines))

(defn get-listing-at-path [path send-http]
  (get-listing send-http (build-uri (get-host-address) path)))

(defn get-ec2-instance-data [send-http]
  (some-> (build-path dynamic-data-root instance-identity-document)
          (get-data-at-path send-http)
          (u/json->edn)))

(defn get-ec2-instance-region [send-http]
  (:region (get-ec2-instance-data send-http)))

(defn container-credentials [send-http]
  (let [endpoint (or (when-let [path (u/getenv container-credentials-relative-uri-env-var)]
                       (str (get-host-address) path))
                     (u/getenv container-credentials-full-uri-env-var))]
    (some-> endpoint (get-data send-http) (u/json->edn))))

(defn instance-credentials [send-http]
  (when (not (in-container?))
    (when-let [cred-name (first (get-listing-at-path security-credentials-path send-http))]
      (some-> (get-data-at-path (str security-credentials-path cred-name) send-http)
              (u/json->edn)))))
