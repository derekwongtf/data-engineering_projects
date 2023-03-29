set -e

# http://data.insideairbnb.com/china/hk/hong-kong/2022-12-23/data/listings.csv.gz

YEAR=$1 # 2022

LOCATION="china/hk/hong-kong"

URL_PREFIX="http://data.insideairbnb.com"

for MONTH in {3..12..3}; do
  FMONTH=`printf "%02d" ${MONTH}`
  
  for DAY in {1..31}; do
    FDAY=`printf "%02d" ${DAY}`
    URL="${URL_PREFIX}/${LOCATION}/${YEAR}-${FMONTH}-${FDAY}/data/listings.csv.gz"
    
    LOCAL_PREFIX="data/raw/${LOCATION}/${YEAR}/${FMONTH}"
    LOCAL_FILE="listings_${YEAR}${FMONTH}${FDAY}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    if [[ `wget -S --spider ${URL} 2>&1 | grep 'HTTP/1.1 200 OK'` ]]; then 
     echo "downloading ${URL} to ${LOCAL_PATH}"
     mkdir -p ${LOCAL_PREFIX}
     wget ${URL} -O ${LOCAL_PATH}
    fi
  done
done