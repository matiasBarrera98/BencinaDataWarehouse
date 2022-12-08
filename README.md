# BencinaDataWarehouse
Proyecto de crear un datawarehouse a partir de la API de la Comisión Nacional de Energía (CNE), 
que contiene información sobre bencineras a lo largo de Chile, precios y otros servicios.

El objetivo es crear un sistema ETL que se ejecute automáticamente en la nube de forma diaria.
Para esto, se usó una CloudFunction de GCP, donde se subieron los archivos main.py y requirements.txt, 
para finalmente automatizar la ejecución de la función con un trabajo en CloudScheduler.
Como plataforma para el datawarehouse se usó Bigquery con un modelo estrella.
