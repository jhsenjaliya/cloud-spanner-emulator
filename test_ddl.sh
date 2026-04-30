#!/usr/bin/env bash
# Test DDL with TOKENIZE_FULLTEXT remove_diacritics fix
set -euo pipefail

CONTAINER_NAME="spanner-emulator-ddl-test"
IMAGE="spanner-emulator-test"
PROJECT="test-project"
INSTANCE="test-instance"
DATABASE="test-db"

cleanup() {
  echo "Cleaning up..."
  docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting emulator ==="
docker run -d --name "$CONTAINER_NAME" -p 9010:9010 -p 9020:9020 "$IMAGE"
sleep 3

echo "=== Creating instance ==="
curl -s -X POST "http://localhost:9020/v1/projects/${PROJECT}/instances" \
  -H 'Content-Type: application/json' \
  -d "{
    \"instanceId\": \"${INSTANCE}\",
    \"instance\": {
      \"config\": \"emulator-config\",
      \"displayName\": \"Test Instance\",
      \"nodeCount\": 1
    }
  }" | python3 -m json.tool 2>/dev/null || echo "(instance may already exist)"

echo ""
echo "=== Creating database ==="
curl -s -X POST "http://localhost:9020/v1/projects/${PROJECT}/instances/${INSTANCE}/databases" \
  -H 'Content-Type: application/json' \
  -d "{
    \"createStatement\": \"CREATE DATABASE \`${DATABASE}\`\"
  }" | python3 -m json.tool 2>/dev/null || echo "(database may already exist)"

sleep 1

echo ""
echo "=== Applying DDL (the full table with TOKENIZE_FULLTEXT remove_diacritics) ==="
DDL_RESPONSE=$(curl -s -X PATCH \
  "http://localhost:9020/v1/projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE}/ddl" \
  -H 'Content-Type: application/json' \
  -d '{
    "statements": [
      "CREATE TABLE P2P_USER_PEERS_SV4 ( PEER_ID INT64 NOT NULL, NETWORK_TYPE STRING(16) NOT NULL, ENTITY_TYPE STRING(16) NOT NULL, ENTITY_INTERNAL_ID STRING(64) NOT NULL, ENTITY_EXTERNAL_ID STRING(64), ENTITY_EXTERNAL_HANDLE STRING(64), ENTITY_PHOTO_URL STRING(1024), ENTITY_FIRST_NAME STRING(64), ENTITY_MIDDLE_NAME STRING(64), ENTITY_LAST_NAME STRING(64), ENTITY_BUSINESS_NAME STRING(64), ENTITY_CITY STRING(64), ENTITY_STATE STRING(64), ENTITY_COUNTRY STRING(2), PUBLIC_SEARCH_ENABLED STRING(1) NOT NULL, PHONE_SEARCH_ENABLED STRING(1) NOT NULL, EMAIL_SEARCH_ENABLED STRING(1) NOT NULL, PRIVACY_CONTROL_FLAG INT64, STATUS STRING(16) NOT NULL, TIME_CREATED INT64 NOT NULL, TIME_UPDATED INT64 NOT NULL, PRIVACY_PREFERENCE_TYPE STRING(16), NameSubstring STRING(MAX) AS (CASE WHEN NETWORK_TYPE='\''PAYPAL'\'' AND ENTITY_TYPE='\''USER'\'' AND PUBLIC_SEARCH_ENABLED = '\''1'\'' AND status = '\''ACTIVE'\'' THEN COALESCE(ENTITY_FIRST_NAME, '\'''\'') || '\'' '\'' || COALESCE(ENTITY_LAST_NAME, '\'''\'') ELSE NULL END) STORED, LocationSubstring STRING(MAX) AS (CASE WHEN NETWORK_TYPE='\''PAYPAL'\'' AND ENTITY_TYPE='\''USER'\'' AND PUBLIC_SEARCH_ENABLED = '\''1'\'' AND status = '\''ACTIVE'\'' THEN COALESCE(ENTITY_CITY, '\'''\'') || '\'' '\'' || COALESCE(ENTITY_STATE, '\'''\'') || '\'' '\'' || COALESCE(ENTITY_COUNTRY, '\'''\'') ELSE NULL END) STORED, Name_loc STRING(MAX) AS (COALESCE(NameSubstring, '\'''\'') || '\'' '\'' || COALESCE(LocationSubstring, '\'''\'')) STORED, Fuzzy_Name_Tkn TOKENLIST AS (TOKENIZE_NGRAMS(LOWER(NameSubstring), ngram_size_min => 3, ngram_size_max => 6, remove_diacritics => TRUE)) HIDDEN, Substr_Name_Tkn TOKENLIST AS (TOKENIZE_SUBSTRING(NameSubstring, support_relative_search => TRUE, ngram_size_min => 3, ngram_size_max => 6, remove_diacritics => TRUE)) HIDDEN, Fulltxt_Name_Tkn TOKENLIST AS (TOKENIZE_FULLTEXT(NameSubstring, remove_diacritics => TRUE)) HIDDEN, Location_Tkn TOKENLIST AS (TOKENIZE_SUBSTRING(LocationSubstring, support_relative_search => TRUE, ngram_size_min => 2, ngram_size_max => 6, remove_diacritics => TRUE)) HIDDEN, Fuzzy_Name_loc_Tkn TOKENLIST AS (TOKENIZE_NGRAMS(LOWER(Name_loc), ngram_size_min => 3, ngram_size_max => 6, remove_diacritics => TRUE)) HIDDEN, Bussiness_Name STRING(MAX) AS (CASE WHEN NETWORK_TYPE='\''PAYPAL'\'' AND ENTITY_TYPE='\''USER'\'' AND PUBLIC_SEARCH_ENABLED = '\''1'\'' AND status = '\''ACTIVE'\'' AND ENTITY_BUSINESS_NAME IS NOT NULL THEN LOWER(ENTITY_BUSINESS_NAME) ELSE NULL END) STORED, Bussiness_Name_Tkn TOKENLIST AS (TOKENIZE_NGRAMS(Bussiness_Name, ngram_size_min => 3, ngram_size_max => 6, remove_diacritics => TRUE)) HIDDEN, norm_first STRING(64) AS (REGEXP_REPLACE(REGEXP_REPLACE(LOWER(NORMALIZE(COALESCE(ENTITY_FIRST_NAME, '\'''\''), NFD)), r'\''\\pM'\'', '\'''\''), r'\''[-\\s]+'\'', '\'''\'')) STORED, norm_last STRING(64) AS (REGEXP_REPLACE(REGEXP_REPLACE(LOWER(NORMALIZE(COALESCE(ENTITY_LAST_NAME, '\'''\''), NFD)), r'\''\\pM'\'', '\'''\''), r'\''[-\\s]+'\'', '\'''\'')) STORED, Firstname_Soundex STRING(4) AS (CASE WHEN NETWORK_TYPE='\''PAYPAL'\'' AND ENTITY_TYPE='\''USER'\'' AND PUBLIC_SEARCH_ENABLED='\''1'\'' AND status='\''ACTIVE'\'' THEN SOUNDEX(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(NORMALIZE(COALESCE(ENTITY_FIRST_NAME, '\'''\''), NFD)), r'\''\\pM'\'', '\'''\''), r'\''[-\\s]+'\'', '\'''\'')) ELSE NULL END) STORED, Lastname_Soundex STRING(4) AS (CASE WHEN NETWORK_TYPE='\''PAYPAL'\'' AND ENTITY_TYPE='\''USER'\'' AND PUBLIC_SEARCH_ENABLED='\''1'\'' AND status='\''ACTIVE'\'' THEN SOUNDEX(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(NORMALIZE(COALESCE(ENTITY_LAST_NAME, '\'''\''), NFD)), r'\''\\pM'\'', '\'''\''), r'\''[-\\s]+'\'', '\'''\'')) ELSE NULL END) STORED, Firstname_Soundex_Tkn TOKENLIST AS (TOKEN(Firstname_Soundex)) HIDDEN, Lastname_Soundex_Tkn TOKENLIST AS (TOKEN(Lastname_Soundex)) HIDDEN ) PRIMARY KEY (PEER_ID)"
    ]
  }')

echo "$DDL_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$DDL_RESPONSE"

# Check for errors
if echo "$DDL_RESPONSE" | grep -qi "error"; then
  echo ""
  echo "!!! DDL FAILED !!!"
  exit 1
fi

echo ""
echo "=== Verifying table exists ==="
sleep 2
DDL_CHECK=$(curl -s "http://localhost:9020/v1/projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE}/ddl")
if echo "$DDL_CHECK" | grep -q "P2P_USER_PEERS_SV4"; then
  echo "SUCCESS: Table P2P_USER_PEERS_SV4 created!"
  echo "$DDL_CHECK" | python3 -m json.tool 2>/dev/null | head -5
else
  echo "FAILED: Table not found in DDL"
  echo "$DDL_CHECK"
  exit 1
fi

echo ""
echo "=== ALL TESTS PASSED ==="
