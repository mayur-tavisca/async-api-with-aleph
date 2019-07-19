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


(defn invoke-init-api []
  (-> @(http/post
         "https://public-be.oski.io/hotel/v1.0/search/init"
         {:headers request-header
          :body    (json/generate-string init-request-body)})
      :body
      bs/to-string
      (json/parse-string keyword)
      ))

(defn invoke-status-check-api [request-body]
  (-> @(http/post
         "https://public-be.oski.io/hotel/v1.0/search/status"
         {:headers request-header
          :body    (json/generate-string request-body)})
      :body
      bs/to-string
      (json/parse-string keyword))
  )

(defn invoke-search-hotel-api [sessionId]
  (-> @(http/post
             "https://public-be.oski.io/hotel/v1.0/search/results"
             {:headers request-header
              :body    (-> (merge sessionId hotel-search-request-body)
                           json/generate-string)
              })
           :body
           bs/to-string))

(defn- initiate-request [target-chan]
  (let [sessionId (invoke-init-api)
        channel (timeout 10000)]
    (go-loop [status "InProgress"]
      (if (= status "Complete")
        (>! channel "ready")
        (recur (-> (invoke-status-check-api sessionId)
                   :status))))
    (go
      (cond
        (= "ready" (<! channel)) (>! target-chan {:status 200
                                  :headers {"content-type" "application/json"}
                                  :body (invoke-search-hotel-api sessionId)})
        (nil? (<! channel)) (>! target-chan {:status 200
                             :body "Timeout"}))))
  )

(defn handler [req]
  (let [target-channel (chan)]
    (initiate-request target-channel)
    (<!! target-channel))
  )

(http/start-server handler {:port 9876})






