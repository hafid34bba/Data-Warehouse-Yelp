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
