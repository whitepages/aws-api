(ns cognitect.aws.util-test
  (:require [byte-streams :as byte-streams]
            [clojure.test :refer :all]
            [cognitect.aws.util :as util])
  (:import java.nio.ByteBuffer))

(deftest test-sha-256
  (testing "returns sha for empty string if given nil"
    (is (= (seq (util/sha-256 nil))
           (seq (util/sha-256 ""))
           (seq (util/sha-256 (.getBytes ""))))))
  (testing "accepts string, byte array, or ByteBuffer"
    (is (= (seq (util/sha-256 "hi"))
           (seq (util/sha-256 (.getBytes "hi")))
           (seq (util/sha-256 (ByteBuffer/wrap (.getBytes "hi")))))))
  (testing "does not consume a ByteBuffer"
    (let [bb (ByteBuffer/wrap (.getBytes "hi"))]
      (util/sha-256 bb)
      (is (= "hi" (byte-streams/to-string bb))))))

(deftest test-xml-read
  (testing "removes whitespace-only nodes, preserving whitespace in single text nodes"
    (let [parsed (util/xml-read "<outer>
                                  outer-value
                                  <inner>
                                     inner-value
                                  </inner>
                                </outer>")]
      (is (= 2 (count (-> parsed :content))))
      (is (re-matches #"\n\s+outer-value\s+" (-> parsed :content first)))
      (is (= 1 (count (-> parsed :content last :content))))
      (is (re-matches #"\n\s+inner-value\s+" (-> parsed :content last :content first))))))

(comment
  (run-tests)

  )