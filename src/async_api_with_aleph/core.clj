(ns async-api-with-aleph.core
  (:require
    [aleph.http :as http]
    [clojure.core.async :refer [go chan timeout]]
    [cheshire.core :as json]
    [manifold.deferred :as d]
    [byte-streams :as bs]
    [clojure.core.async :refer [go go-loop <! >! timeout <!!]]
    [manifold.stream :as s]))

(def init-request-body {:includeHotelsWithoutRates       false,
                        :travellerCountryCodeOfResidence "US",
                        :filters                         {:minHotelPrice  1,
                                                          :maxHotelPrice  10000,
                                                          :minHotelRating 1,
                                                          :maxHotelRating 5,
                                                          :hotelChains    ["Novotel" "Marriott" "Hilton" "Accor"],
                                                          :allowedCountry "FR"},
                        :bounds                          {:rectangle
                                                          {:topLeft
                                                                        {:lat  49.014173,
                                                                         :long 2.300387},
                                                           :bottomRight {:lat 48.804093, :long 2.639246}}},
                        :currency                        "USD",
                        :roomOccupancies                 [{:occupants [{:type "Adult", :age 25}]}],
                        :stayPeriod                      {:start "2019-08-18", :end "2019-08-21"},
                        :posId                           "hbg3h7rf28",
                        :travellerNationalityCode        "US",
                        :orderBy                         "price asc, rating desc"})


(def hotel-search-request-body {:paging            {:pageNo   1,
                                                    :pageSize 10,
                                                    :orderBy  "price asc, rating desc"},
                                :optionalDataPrefs ["All"],
                                :currency          "USD",
                                :contentPrefs      ["Basic" "Activities" "Amenities" "Policies" "AreaAttractions" "Descriptions" "Images" "CheckinCheckoutPolicy" "All"],
                                :filters           {:minHotelPrice  1,
                                                    :maxHotelPrice  10000,
                                                    :minHotelRating 1,
                                                    :maxHotelRating 5,
                                                    :hotelChains    ["Novotel" "Marriott" "Hilton" "Accor"], :allowedCountry "FR"}})

(def request-header {:oski-tenantId "Demo"
                     :Content-Type  "application/json"})

(defn prepare-response [status response]
  {:status  status
   :headers {"content-type" "application/json"}
   :body    response})

(def terminating-statuses #{"Complete" "Error" "Timeout"})


(defn invoke-init-api []
  (d/chain (http/post
             "https://public-be.oski.io/hotel/v1.0/search/init"
             {:headers request-header
              :body    (json/generate-string init-request-body)})
           :body
           bs/to-string
           #(json/parse-string % keyword)
           ))

(defn invoke-status-check-api [request-body]
  (d/chain (http/post
             "https://public-be.oski.io/hotel/v1.0/search/status"
             {:headers request-header
              :body    (json/generate-string request-body)})
           :body
           bs/to-string
           #(json/parse-string % keyword)
           :status))

(defn invoke-search-hotel-api [sessionId]
  (d/chain (http/post
             "https://public-be.oski.io/hotel/v1.0/search/results"
             {:headers request-header
              :body    (-> (merge sessionId hotel-search-request-body)
                           json/generate-string)})
           :body
           bs/to-string))

(defn check-hotel-search-status [status-atom session-id timeout-chan]
  (go-loop [status @status-atom status-check-deferred (invoke-status-check-api session-id)]
    (if (terminating-statuses status)
      (do
        (println "go-loop terminates with status " status)
        (>! timeout-chan status))
      (recur (do (<! (timeout 1000))
                 (d/on-realized status-check-deferred
                                (fn [status]
                                  (when-not (= @status-atom "Timeout")
                                    (reset! status-atom status)))
                                (fn [error]
                                  (println "error occurred:" error)
                                  (reset! status-atom "Error")))
                 (println "inside recur " @status-atom)
                 (deref status-atom)) (invoke-status-check-api session-id)))))

(defn initiate-request [target-chan]
  (let [timeout-chan (timeout 10000)
        sessionId (atom {})
        init-deferred (invoke-init-api)
        status-atom (atom "Not-Started")]

    (d/on-realized init-deferred
                   (fn [session-id]
                     (reset! sessionId session-id)
                     (check-hotel-search-status status-atom session-id timeout-chan))
                   (fn [error]
                     (println "error occurred on realizing init. Error: " error)
                     (reset! status-atom "Error")
                     ))
    (go
      (cond
        (= "Complete" (<! timeout-chan)) (>! target-chan
                                             (prepare-response 200 (invoke-search-hotel-api @sessionId)))
        (nil? (<! timeout-chan)) (do
                                   (reset! status-atom "Timeout")
                                   (>! target-chan
                                       (prepare-response 206 (invoke-search-hotel-api @sessionId))))

        (= "Error" (<! timeout-chan)) (>! target-chan
                                          (prepare-response 500 (invoke-search-hotel-api @sessionId)))))))

(defn handler [req]
  (let [target-channel (chan)]
    (initiate-request target-channel)
    (<!! target-channel)))

(defonce start-server (http/start-server #'handler {:port 9876}))













