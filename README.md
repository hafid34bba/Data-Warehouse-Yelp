# Data-Warehouse-Yelp

## Analyse 
L'analyse de ce sujet est basé sur des avis d’utilisateurs sur de nombreuses commerces, l'objectif est d'étendre ce sujet en plusieurs analyses  sur différents axes : 

- **Profil utilisateur "user"** : 
    les analyses seront basés sur l'axe des utilisateurs, qui ont un dépendance entre celles - ci.
- **Profil commerce "business"** : 
    ce profil business est un peu complexe, nous allons essayer d'expliquer et répresenter le mieux dans notre schéma de modelisation pour la construction de ce data mart.
    
## Approche

Sachant que notre objectif est de développer une solution de gestion de données conviviale pour les utilisateurs finaux, qui permet une analyse rapide et interactive des données, nous avons opté pour l'architecture de Kimball.<br>

![](images/DataMarts.png)

## Schéma de data warehouse et data mart

Dans ce schéma, il y a deux tables de faits : "commerces" (en flocon) et "utilisateurs" (en étoile) avec plusieurs dimensions.

![](images/shéma.png)

## ETL : 
Pour la construction de notre data warehouse, nous devons passer par un processus **ETL** Extract-Load-Transform.

![](images/etl.png)

- **Extraction des données :**  
Dans cette étape, nous avons des données qu'on récupère de différentes types : **csv** : tip.csv, **json** : business.json, checkin.json, et une base de données **Postgres** qui contient les différentes tables : user, review, elite, friend. Nous avons chargé ses different données dans des dataframes.

- **Transofrmation :** 
  - Nettoyage des données 
  - Extraction des connaissaces à partir des données existantes
  - Discrétisation
  - Encodage des attributs non numériques
  - Jointures entre les différentes sources de données
  - Agrégation en fonction des critères

- **Chargement** 
Après la transformation, nous stockons nos données reparties dans les différentes dataframes **business-fact, user-fact, dim-business, etc...** dans une base de données **Oracle** afin de visualiser nos tables de faits et de dimensions.
Avant de charger les tables finales, nous avons passé par une zone de stockage temporaire. Nous avons par suite crée un script SQL pour terminer la création de la table de fait commerce (business) ainsi que la création de la table de dimension trimestre.




