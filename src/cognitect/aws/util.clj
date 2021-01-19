;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.util
  "Impl, don't call directly."
  (:require [clojure.string :as str]
            [clojure.data.xml :as xml]
            [jsonista.core :as json]
            [byte-streams :as byte-streams]
            [clojure.core.async :as a]
            [clojure.java.io :as io])
  (:import [java.util.concurrent Executors ExecutorService ThreadFactory]
           [java.text SimpleDateFormat]
           [java.util Date TimeZone]
           [java.util UUID]
           [java.io InputStream]
           [java.nio.charset Charset]
           [java.security MessageDigest]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.net URLEncoder]
           [java.util Base64]))

(set! *warn-on-reflection* true)

(defn ^ThreadLocal date-format
  "Return a thread-safe GMT date format that can be used with `format-date` and `parse-date`.

  See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4228335"
  [^String fmt]
  (proxy [ThreadLocal] []
    (initialValue []
      (doto (SimpleDateFormat. fmt)
        (.setTimeZone (TimeZone/getTimeZone "GMT"))))))

(defn format-date
  ([fmt]
   (format-date fmt (Date.)))
  ([^ThreadLocal fmt inst]
   (.format ^SimpleDateFormat (.get fmt) inst)))

(defn format-timestamp
  "Format a timestamp in milliseconds."
  [inst]
  (str (long (/ (.getTime ^Date inst) 1000))))

(defn parse-date
  [^ThreadLocal fmt s]
  (.parse ^SimpleDateFormat (.get fmt) s))

(def ^ThreadLocal x-amz-date-format
  (date-format "yyyyMMdd'T'HHmmss'Z'"))

(def ^ThreadLocal x-amz-date-only-format
  (date-format "yyyyMMdd"))

(def ^ThreadLocal iso8601-date-format
  (date-format "yyyy-MM-dd'T'HH:mm:ssXXX"))

(def ^ThreadLocal iso8601-msecs-date-format
  (date-format "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

(def ^ThreadLocal rfc822-date-format
  (date-format "EEE, dd MMM yyyy HH:mm:ss z"))

(let [hex-chars (char-array [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f])]
  (defn hex-encode
    [^bytes bytes]
    (let [bl (alength bytes)
          ca (char-array (* 2 bl))]
      (loop [i (int 0)
             c (int 0)]
        (if (< i bl)
          (let [b (long (bit-and (long (aget bytes i)) 255))]
            (aset ca c ^char (aget hex-chars (unsigned-bit-shift-right b 4)))
            (aset ca (unchecked-inc-int c) (aget hex-chars (bit-and b 15)))
            (recur (unchecked-inc-int i) (unchecked-add-int c 2)))
          (String. ca))))))

(defn sha-256
  "Returns the sha-256 digest (bytes) of data, which can be a
  byte-array, an input-stream, or nil, in which case returns the
  sha-256 of the empty string."
  [data]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (when data
      (.update digest ^bytes (byte-streams/to-byte-array data)))
    (.digest digest)))

(defn hmac-sha-256
  [key ^String data]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. key "HmacSHA256"))
    (.doFinal mac (.getBytes data "UTF-8"))))

(defn xml-read
  "Parse the UTF-8 XML string."
  [x]
  (-> x
      (byte-streams/to-string)
      (xml/parse-str :namespace-aware false :skip-whitespace true)))

(defn xml->map [element]
  (cond
    (nil? element)        nil
    (string? element)     element
    (sequential? element) (if (> (count element) 1)
                            (into {} (map xml->map) element)
                            (xml->map (first element)))
    (map? element)
    (cond
      (empty? element) {}
      (:attrs element) {(:tag element)                                (xml->map (:content element))
                        (keyword (str (name (:tag element)) "Attrs")) (:attrs element)}
      :else            {(:tag element) (xml->map  (:content element))})
    :else                 nil))

(defn error-code [response-body]
  (let [error (some->> (tree-seq coll? #(if (map? %) (vals %) %) response-body)
                       (filter :Error)
                       first)]
    (some-> error (get-in [:Error :Code]))))

(defn xml-write
  [e]
  (if (instance? String e)
    (print e)
    (do
      (print (str "<" (name (:tag e))))
      (when (:attrs e)
        (doseq [attr (:attrs e)]
          (print (str " " (name (key attr)) "=\"" (val attr)"\""))))
      (if-not (empty? (:content e))
        (do
          (print ">")
          (doseq [c (:content e)]
            (xml-write c))
          (print (str "</" (name (:tag e)) ">")))
        (print " />")))))

(defn url-encode
  "Percent encode the string to put in a URL."
  [^String s]
  (-> s
      (URLEncoder/encode "UTF-8")
      (.replace "+" "%20")))

(defn query-string
  "Create a query string from a list of parameters. Values must all be
  strings."
  [params]
  (when-not (empty? params)
    (str/join "&" (map (fn [[k v]]
                         (str (url-encode (name k))
                              "="
                              (url-encode v)))
                       params))))

(def json-object-mapper
  (json/object-mapper {:decode-key-fn true}))

(defn json->edn [obj]
  (json/read-value obj json-object-mapper))

(defn read-json
  "Read readable as JSON. readable can be any valid input for
  clojure.java.io/reader."
  [readable]
  (-> readable json->edn))

(defn map-vals
  "Apply f to the values with the given keys, or all values if `ks` is not specified."
  ([f m]
   (map-vals f m (keys m)))
  ([f m ks]
   (into m
         (for [[k v] (select-keys m ks)]
           [k (f v)]))))

(defprotocol Base64Encodable
  (base64-encode [data]))

(extend-protocol Base64Encodable
  (class (byte-array 0))
  (base64-encode [ba] (.encodeToString (Base64/getEncoder) ba))

  ByteBuffer
  (base64-encode [bb] (base64-encode (.array bb)))

  java.lang.String
  (base64-encode [s] (base64-encode (.getBytes s))))

(defn base64-decode
  "base64 decode a base64-encoded string to an input stream"
  [s]
  (io/input-stream (.decode (Base64/getDecoder) ^String s)))

(defn encode-jsonvalue [data]
  (base64-encode (json/write-value-as-bytes data)))

(defn parse-jsonvalue [data]
  (-> data
      base64-decode
      io/reader
      json->edn))

(def ^Charset UTF8 (Charset/forName "UTF-8"))

(defn ^bytes md5
  "returns an MD5 hash of the content of bb as a byte array"
  [^ByteBuffer bb]
  (let [ba     (.array bb)
        hasher (MessageDigest/getInstance "MD5")]
    (.update hasher ^bytes ba)
    (.digest hasher)))

(defn uuid-string
  "returns a string representation of a randomly generated UUID"
  []
  (str (UUID/randomUUID)))

(defn with-defaults
  "Given a shape and data of that shape, add defaults for the
  following required keys if they are missing or bound to nil

      :idempotencyToken"
  [shape data]
  (reduce (fn [m [member-name member-spec]]
            (cond
              (not (nil? (get data member-name)))
              m

              (:idempotencyToken member-spec)
              (assoc m member-name (uuid-string))

              :else
              m))
          (or data {})
          (:members shape)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; used to fetch creds and region

(defn fetch-async
  "Internal use. Do not call directly."
  [fetch provider item]
  (a/thread
    (try
      ;; lock on the provider to avoid redundant concurrent requests
      ;; before the provider has a chance to cache the results of the
      ;; first fetch.
      (or (locking provider
            (fetch provider))
          {:cognitect.anomalies/category :cognitect.anomalies/fault
           :cognitect.anomalies/message (format "Unable to fetch %s. See log for more details." item)})
      (catch Throwable t
        {:cognitect.anomalies/category :cognitect.anomalies/fault
         ::throwable t
         :cognitect.anomalies/message (format "Unable to fetch %s." item)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Wrappers - here to support testing with-redefs since
;;;;            we can't redef static methods

(defn getenv
  ([] (System/getenv))
  ([k] (System/getenv k)))

(defn getProperty [k]
  (System/getProperty k))
