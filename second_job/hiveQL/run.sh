#!/bin/bash

# Definisci le dimensioni dei dataset
SIZES=("bench_50" "bench_100" "bench_200" "bench_400")

# Percorsi base (S3)
S3_BUCKET="s3://bigdata2026bucket-giacomo-cui"
LOCAL_HQL="/tmp/esercitazione_3_2_hive.hql"

echo "Avvio della suite di test completa..."

# 1. SCARICA IL FILE HQL DA S3 ALLA CARTELLA LOCALE DEL MASTER NODE
# In questo modo Hive potrà leggerlo correttamente tramite il parametro -f
echo "Download dello script HQL da S3..."
aws s3 cp "$S3_BUCKET/scripts/esercitazione_3_2_hive.hql" "$LOCAL_HQL"

if [ $? -ne 0 ]; then
    echo "ERRORE: Impossibile scaricare lo script HQL da S3. Verifica il percorso."
    exit 1
fi

for SIZE in "${SIZES[@]}"
do
    echo "--------------------------------------------------------"
    echo "TESTING DATASET: $SIZE"
    echo "--------------------------------------------------------"

    INPUT_PATH="$S3_BUCKET/data/$SIZE/"
    OUTPUT_PATH="$S3_BUCKET/output/hive/$SIZE"

    # Pulizia della cartella di output su S3 prima del run per evitare conflitti
    echo "Pulizia vecchia cartella di output S3 per $SIZE..."
    aws s3 rm "$OUTPUT_PATH" --recursive 2>/dev/null

    # 3. Esecuzione Hive
    echo "Esecuzione Hive con Tez su EMR..."

    # Passiamo i path S3 corretti e puntiamo al file .hql salvato localmente in /tmp
    hive -hiveconf INPUT_PATH="$INPUT_PATH" \
         -hiveconf OUTPUT_PATH="$OUTPUT_PATH" \
         -f "$LOCAL_HQL"

    echo "Terminato test per $SIZE"
done

echo "Tutti i benchmark sono stati completati con successo!"

# 4. RACCOLTA DEI LOG/REPORT FINALI DA S3
# Scarichiamo temporaneamente i risultati localmente per poter usare grep
echo "Generazione del report finale delle performance..."
mkdir -p /tmp/hive_outputs
aws s3 cp "$S3_BUCKET/output/hive/" /tmp/hive_outputs/ --recursive --include "*.csv" --include "*0000*" 2>/dev/null

# Esegui il grep sui file scaricati localmente
grep -r "total_time_s" /tmp/hive_outputs/ > report_finale_performance.txt
echo "Report salvato in report_finale_performance.txt"

# Pulizia file temporanei locali
rm -rf /tmp/hive_outputs
rm -f "$LOCAL_HQL"