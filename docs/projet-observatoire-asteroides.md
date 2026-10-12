# Observatoire des astéroïdes proches de la Terre

Projet de data engineering : pipeline ELT sur GCP alimentée par l'API NASA NeoWs.

---

## 1. Résumé du projet

**Objectif.** Construire de bout en bout une pipeline de données automatisée qui collecte chaque jour les astéroïdes passant près de la Terre, les stocke de façon fiable, les transforme en un entrepôt analytique propre et les expose dans un tableau de bord.

**Ce que le projet démontre :**
- ingestion robuste d'une API (pagination, limites de requêtes, retries, validation) ;
- architecture ELT moderne avec conservation de la donnée brute ;
- modélisation en couches (raw → staging → marts) avec dbt ;
- tests de qualité des données ;
- orchestration, reprise sur historique (backfill) et idempotence ;
- infrastructure as code (Terraform) et CI/CD ;
- maîtrise du cloud (GCP) avec un coût proche de zéro.

**Contraintes de départ :**
- accès à GCP jusqu'à la fin de l'année 2026 : tout doit être reproductible et démontrable après la coupure ;
- stocker le moins de données possible sur la machine locale : tout s'exécute dans le cloud ;
- projet à finir en environ 5 semaines, avec de la marge avant la fin de l'accès GCP.

**Choix d'architecture : ELT.** La donnée brute (JSON) est déposée telle quelle dans Cloud Storage, chargée dans BigQuery, puis transformée en SQL avec dbt. On peut ainsi rejouer toutes les transformations sans rappeler l'API.

---

## 2. Données utilisées

### Source : NASA NeoWs (Near Earth Object Web Service)

- Portail : https://api.nasa.gov (section « NeoWs »)
- Authentification : clé API gratuite (à demander sur le portail). La `DEMO_KEY` est très limitée.
- Limites indicatives (à vérifier sur le portail au moment du projet) : environ 30 requêtes/heure et 50/jour avec `DEMO_KEY` ; environ 1 000 requêtes/heure avec une clé personnelle.
- Données sous licence ouverte de la NASA (usage libre, mention de la source recommandée).

### Endpoints

| Endpoint | Usage dans le projet | Remarques |
|---|---|---|
| `/neo/rest/v1/feed?start_date=&end_date=` | **Source principale** : astéroïdes dont le passage tombe dans une fenêtre de dates | Fenêtre maximale de 7 jours par appel |
| `/neo/rest/v1/neo/{id}` | **Enrichissement (optionnel)** : détail d'un astéroïde, historique complet de ses passages, données orbitales | Un appel par objet, à utiliser avec parcimonie |
| `/neo/rest/v1/neo/browse?page=&size=` | **Catalogue complet (optionnel)** : alimentation initiale de la dimension astéroïdes | Paginé, volumineux |

### Champs principaux (feed)

Pour chaque astéroïde :
- `id`, `neo_reference_id`, `name`, `nasa_jpl_url`
- `absolute_magnitude_h`
- `estimated_diameter` (min/max en kilomètres, mètres, miles, pieds)
- `is_potentially_hazardous_asteroid` (booléen)
- `is_sentry_object` (booléen)

Pour chaque passage (`close_approach_data`, liste) :
- `close_approach_date`, `close_approach_date_full`, `epoch_date_close_approach`
- `relative_velocity` (km/s, km/h, mph)
- `miss_distance` (astronomical, lunar, kilometers, miles)
- `orbiting_body` (en général `Earth`)

Via l'endpoint de détail (optionnel) : `orbital_data` (classe orbitale, excentricité, demi-grand axe, inclinaison, période orbitale, etc.).

### Volumétrie attendue

Quelques dizaines d'objets par jour, soit quelques centaines de Mo au maximum avec plusieurs années d'historique. Aucun besoin de calcul distribué.

---

## 3. Architecture

```
Cloud Scheduler (quotidien)
        │
        ▼
Cloud Workflows (orchestration : ingestion → chargement → dbt)
        │
        ▼
Cloud Run Job (Python : httpx + tenacity + pydantic)
        │  appelle NeoWs
        ▼
Cloud Storage : raw/neows/feed/date=YYYY-MM-DD/*.json   (JSON brut, rejouable)
        │
        ▼
BigQuery : raw/staging  →  intermediate  →  marts       (dbt)
        │
        ▼
Looker Studio (tableau de bord) + analyse statistique (R ou Python)
```

Principe « zéro stockage local » : le code est la seule chose qui vit sur la machine. Les données ne transitent jamais par elle.

---

## 4. Outils et justification

| Couche | Outil | Rôle |
|---|---|---|
| Langage / environnement | Python + **uv** | Code d'ingestion, gestion des dépendances |
| Appels API | **httpx** + **tenacity** | Requêtes HTTP, retries avec backoff, gestion du rate limiting |
| Validation | **pydantic** | Schémas des réponses, rejet des données invalides |
| Exécution | **Cloud Run Jobs** | Exécution serverless du code d'ingestion |
| Déclenchement | **Cloud Scheduler** | Lancement quotidien |
| Orchestration | **Cloud Workflows** | Enchaînement des étapes sans serveur à maintenir |
| Lac de données | **Cloud Storage (GCS)** | Conservation du JSON brut |
| Entrepôt | **BigQuery** | Stockage analytique et calcul |
| Transformations | **dbt-core** (adaptateur BigQuery) | Modèles SQL, tests, documentation, lignage |
| Secrets | **Secret Manager** | Clé API NASA |
| Build d'image | **Cloud Build** + Artifact Registry | Construction de l'image Docker sans la construire en local |
| Infrastructure | **Terraform** | Description reproductible de tout le cloud |
| CI/CD | **GitHub Actions** | Lint, tests, `dbt build` à chaque push |
| Restitution | **Looker Studio** | Tableau de bord gratuit |
| Analyse | **R (bigrquery)** ou Python | Analyse statistique sur les marts |

Outils volontairement écartés : Cloud Composer (trop coûteux), Spark/Dataflow (surdimensionné), Kafka/Pub/Sub (pas de flux temps réel).

---

## 5. Plan détaillé

### Étape 0 — Préparation (2 à 3 jours)
**Résumé :** poser les bases du projet et sécuriser le budget.
- Demander la clé API NASA.
- Créer le projet GCP, activer les APIs nécessaires, configurer la facturation.
- **Créer des alertes de budget** (par exemple 5 €, 10 €, 20 €).
- Créer le dépôt GitHub, la structure du projet et la configuration `uv`.
- Choisir une région unique (par exemple `europe-west1` ou `EU`) pour tous les services.

**Outils :** GCP, GitHub, uv.

### Étape 1 — Infrastructure as code (semaine 1)
**Résumé :** décrire toute l'infrastructure en Terraform.
- Bucket GCS (raw), datasets BigQuery (raw, staging, marts).
- Comptes de service avec le minimum de droits (principe du moindre privilège).
- Secret Manager pour la clé NASA.
- Authentification sans fichier de clé JSON local (`gcloud auth application-default login` en développement).
- Quota de requêtes quotidien sur BigQuery.

**Outils :** Terraform, gcloud, Secret Manager.
**Livrable :** `terraform apply` crée tout, `terraform destroy` supprime tout.

### Étape 2 — Ingestion (semaine 2)
**Résumé :** écrire un client NeoWs fiable.
- Découpage d'une plage de dates en fenêtres de 7 jours maximum.
- Retries avec backoff exponentiel, respect des limites de requêtes, timeouts.
- Validation des réponses avec pydantic.
- Dépôt du JSON brut dans GCS avec un **chemin déterministe** par date (idempotence).
- Journalisation structurée (logs lisibles dans Cloud Logging).
- Tests unitaires avec de petits fichiers d'exemple (fixtures de quelques Ko versionnées dans Git).

**Outils :** Python, httpx, tenacity, pydantic, pytest.
**Livrable :** une commande qui ingère une plage de dates dans GCS, rejouable sans doublon.

### Étape 3 — Déploiement et automatisation (semaine 3)
**Résumé :** faire tourner l'ingestion dans le cloud, tous les jours, sans intervention.
- Dockerfile, build avec Cloud Build, image dans Artifact Registry.
- Cloud Run Job + Cloud Scheduler (exécution quotidienne).
- Cloud Workflows pour enchaîner ingestion, chargement et dbt.
- **Backfill de l'historique** par tranches (en respectant les limites de l'API), avec un mode « plage de dates » paramétrable.
- Chargement GCS → BigQuery (table externe ou load job).

**Outils :** Docker, Cloud Build, Cloud Run Jobs, Cloud Scheduler, Cloud Workflows, BigQuery.
**Livrable :** pipeline automatique qui alimente BigQuery chaque jour.

### Étape 4 — Transformations avec dbt (semaine 4)
**Résumé :** transformer le brut en tables analytiques fiables.
- **Staging** : aplatissement du JSON imbriqué, typage, renommage.
- **Intermediate** : déduplication, conversions d'unités cohérentes.
- **Marts** : tables prêtes pour l'analyse (voir section 6).
- **Tests dbt** : unicité, non-nullité, relations entre tables, valeurs acceptées, cohérence (diamètre min ≤ max, distance et vitesse positives), fraîcheur des données.
- Documentation générée et lignage.

**Outils :** dbt-core, BigQuery SQL.
**Livrable :** `dbt build` passe avec tous les tests au vert.

### Étape 5 — Qualité, CI/CD et restitution (semaine 5)
**Résumé :** industrialiser et présenter.
- GitHub Actions : lint (ruff), tests Python, `dbt build` sur un dataset de test.
- Tableau de bord Looker Studio (voir section 7).
- Analyse statistique sur les marts (R ou Python).
- README avec schéma d'architecture, instructions et captures d'écran.

**Outils :** GitHub Actions, ruff, Looker Studio, R/Python.
**Livrable :** dépôt propre, documenté et démontrable.

### Étape 6 — Clôture avant la fin de l'accès GCP (marge de sécurité)
**Résumé :** préserver la valeur du projet après la coupure.
- Prendre des **captures et une vidéo de démonstration** du pipeline et du tableau de bord.
- Exporter les marts en Parquet/CSV (quelques Mo) dans le dépôt ou en release GitHub, pour une démo hors GCP.
- Prévoir un mode local de démonstration (par exemple DuckDB sur l'export) pour que le projet reste exécutable.
- Exécuter `terraform destroy` **avant la fin de l'année** et vérifier que plus aucune ressource ne tourne.

**Outils :** Terraform, DuckDB (démo locale uniquement), GitHub.

---

## 6. Modèle de données (proposition)

**Couche raw**
- `raw_neows_feed` : JSON brut chargé, avec métadonnées d'ingestion (date, fenêtre demandée, identifiant d'exécution).

**Couche staging**
- `stg_neo_objects` : un objet par ligne.
- `stg_close_approaches` : un passage par ligne (un objet peut avoir plusieurs passages).

**Couche marts**
- `dim_asteroids` : caractéristiques de l'astéroïde (identifiant, nom, magnitude, diamètres estimés, dangerosité potentielle, objet Sentry).
- `fct_close_approaches` : un passage par ligne (date, vitesse, distance en km et en distances lunaires, corps orbité, clé vers l'astéroïde).
- `mart_daily_summary` : agrégats quotidiens (nombre de passages, distance minimale, vitesse moyenne, nombre d'objets potentiellement dangereux).
- `mart_monthly_hazardous` : évolution mensuelle des objets potentiellement dangereux.

**Clés et granularité :**
- clé d'un astéroïde : `id` NeoWs ;
- clé d'un passage : (`id` de l'astéroïde, `epoch_date_close_approach`).

---

## 7. Tableau de bord (idées de visualisations)

- Nombre d'astéroïdes par jour/semaine/mois.
- Top des passages les plus proches (distance en distances lunaires).
- Distribution des diamètres estimés et des vitesses.
- Part des objets classés potentiellement dangereux dans le temps.
- Tableau des prochains passages (7 prochains jours).

**Pistes d'analyse statistique (lien avec ton master) :**
- distribution et loi des distances de passage ;
- relation entre taille, magnitude absolue et vitesse ;
- tendance et saisonnalité du nombre de détections (attention : biais d'observation, la détection s'améliore avec le temps) ;
- test de différence entre objets potentiellement dangereux et les autres.

---

## 8. Points techniques importants

**Idempotence.** Un rejeu ne doit jamais créer de doublons : chemin GCS déterministe par date, `MERGE` ou écrasement de partition dans BigQuery, clés uniques testées dans dbt.

**Backfill.** Prévoir dès le départ un mode « plage de dates » (début, fin) : la même logique sert à l'ingestion quotidienne et à la reprise d'historique.

**Rate limiting.** Pause entre les appels, backoff exponentiel sur les erreurs 429 et 5xx, reprise là où l'on s'est arrêté. Ne jamais commiter la clé API.

**Données corrigées a posteriori.** La NASA peut mettre à jour des estimations (orbites, diamètres). Prévoir une re-ingestion périodique d'une fenêtre glissante récente pour capter ces corrections.

**Partitionnement et clustering BigQuery.** Partitionner `fct_close_approaches` par date de passage, clusteriser par identifiant d'astéroïde : bonne pratique et réduction des coûts.

**Unités.** Choisir une unité de référence (km, km/s, distances lunaires) et convertir une seule fois en staging.

**Observabilité.** Logs structurés, alertes en cas d'échec du job, test de fraîcheur dbt pour détecter une pipeline arrêtée.

---

## 9. Sécurité

- Clé NASA dans Secret Manager, jamais dans le code ni dans Git.
- Aucun fichier de clé de compte de service sur la machine locale ; utiliser l'authentification applicative par défaut en local.
- Comptes de service dédiés avec des droits minimaux.
- `.gitignore` strict (variables d'environnement, fichiers Terraform d'état).
- Pour GitHub Actions : Workload Identity Federation plutôt qu'une clé JSON stockée en secret.

---

## 10. Budget estimé

Coût attendu : **entre 0 et 5 € par mois**, très probablement proche de 0 €, grâce aux quotas gratuits (BigQuery : 1 TiB de requêtes et 10 GiB de stockage par mois, entre autres) et aux faibles volumes.

À vérifier dans le calculateur de prix Google Cloud avant de démarrer, car les quotas gratuits peuvent évoluer.

**Risques de dérive :** Cloud Composer, VM oubliée, boucle d'appels, `SELECT *` répétés sur de grosses tables, régions multiples.

**Garde-fous :** alertes de budget, quota de requêtes BigQuery, `terraform destroy`, contrôle hebdomadaire de la facturation.

---

## 11. Structure du dépôt (proposition)

```
observatoire-asteroides/
├── README.md
├── pyproject.toml
├── uv.lock
├── .github/workflows/ci.yml
├── infra/                  # Terraform
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── src/
│   └── neows_pipeline/
│       ├── client.py       # appels API, retries, rate limiting
│       ├── models.py       # schémas pydantic
│       ├── storage.py      # écriture GCS
│       ├── loader.py       # chargement BigQuery
│       └── main.py         # point d'entrée (Cloud Run Job)
├── tests/
│   ├── fixtures/           # petits JSON d'exemple
│   └── test_*.py
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
├── workflows/              # définition Cloud Workflows
├── Dockerfile
└── docs/
    ├── architecture.png
    └── captures/
```

---

## 12. Valorisation (CV et entretien)

**Ligne de CV possible :**
> Conception d'une pipeline ELT sur GCP (Cloud Run, Cloud Storage, BigQuery, dbt, Terraform) ingérant quotidiennement l'API NASA NeoWs ; tests de qualité, backfill idempotent, CI/CD GitHub Actions, tableau de bord Looker Studio.

**Questions d'entretien à préparer :**
- Pourquoi ELT plutôt qu'ETL ? (entrepôt moderne, conservation du brut, dbt)
- Comment garantis-tu l'idempotence et la reprise sur historique ?
- Comment gères-tu les limites de l'API et les erreurs ?
- Pourquoi Cloud Workflows plutôt qu'Airflow ? Quand choisirais-tu Airflow ?
- Comment contrôles-tu les coûts sur GCP ?
- Quels tests de qualité as-tu mis en place, et pourquoi ceux-là ?

---

## 13. Extensions possibles (si le temps le permet)

- Enrichissement avec l'endpoint de détail (données orbitales).
- Orchestrateur Dagster ou Airflow sur une petite VM, en parallèle de Workflows.
- Alerte (e-mail ou message) en cas de passage très proche.
- Modèle statistique de prédiction ou de détection d'anomalies sur les passages.
- Croisement avec d'autres API NASA (par exemple DONKI) pour la suite du portfolio.

---

## 14. Checklist de fin de projet

- [ ] Alertes de budget actives
- [ ] Infrastructure entièrement décrite en Terraform
- [ ] Pipeline quotidienne automatique et idempotente
- [ ] Backfill de l'historique réalisé
- [ ] Tests dbt et tests Python au vert, CI active
- [ ] Tableau de bord et analyse statistique terminés
- [ ] README avec schéma d'architecture et captures
- [ ] Démo hors GCP disponible (export + vidéo)
- [ ] `terraform destroy` exécuté avant la fin de l'accès GCP
