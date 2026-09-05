# Catalogue Glue pointant vers les données curated déjà produites par
# glue_jobs/curated_job.py — tables déclarées explicitement (pas de
# crawler) : le schéma des 4 tables est stable et connu (voir wiki
# 04_Architecture_Donnees), un crawler ajouterait un coût récurrent et un
# risque de dérive de schéma non maîtrisé, pour un bénéfice nul ici.

resource "aws_glue_catalog_database" "soundpulse" {
  name = "soundpulse"
}

# curated_job.py écrit chaque table sous curated/<table>/date=<snapshot>/
# (partition Hive `date=YYYY-MM-DD`) : la clé de partition Glue doit donc
# s'appeler `date` pour les 4 tables, quel que soit le nom de la colonne
# homonyme dans les données elles-mêmes (ex. `date_snapshot` sur
# fait_popularite).
locals {
  curated_bucket = aws_s3_bucket.datalake.bucket
}

resource "aws_glue_catalog_table" "fait_popularite" {
  name          = "fait_popularite"
  database_name = aws_glue_catalog_database.soundpulse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification       = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.curated_bucket}/curated/fait_popularite/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Aligné sur build_fait_popularite() dans curated_job.py.
    columns {
      name = "date_snapshot"
      type = "string"
    }
    columns {
      name = "id_titre"
      type = "bigint"
    }
    columns {
      name = "id_artiste"
      type = "bigint"
    }
    columns {
      name = "score_popularite"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "dim_titre" {
  name          = "dim_titre"
  database_name = aws_glue_catalog_database.soundpulse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification       = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.curated_bucket}/curated/dim_titre/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Aligné sur build_dim_titre() dans curated_job.py — pas de colonne
    # energie/dansabilite (schéma Spotify révolu), `genre` = id du genre de
    # chart d'origine (voir docstring de curated_job.py).
    columns {
      name = "id_titre"
      type = "bigint"
    }
    columns {
      name = "nom"
      type = "string"
    }
    columns {
      name = "duree"
      type = "int"
    }
    columns {
      name = "bpm"
      type = "double"
    }
    columns {
      name = "gain"
      type = "double"
    }
    columns {
      name = "genre"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "dim_artiste" {
  name          = "dim_artiste"
  database_name = aws_glue_catalog_database.soundpulse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification       = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.curated_bucket}/curated/dim_artiste/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Aligné sur build_dim_artiste() — pas de colonne `genres`, Deezer
    # n'expose pas de genre au niveau artiste (voir docstring curated_job.py).
    columns {
      name = "id_artiste"
      type = "bigint"
    }
    columns {
      name = "nom"
      type = "string"
    }
    columns {
      name = "nb_fan"
      type = "bigint"
    }
    columns {
      name = "nb_album"
      type = "bigint"
    }
  }
}

# dim_date a une vraie colonne `date` (date calendaire, cf. build_dim_date())
# qui entrerait en collision de nom avec la clé de partition `date` (issue
# du chemin S3 `date=YYYY-MM-DD`) : Glue interdit qu'une colonne de
# partition et une colonne de données portent le même nom. Les deux
# valeurs coïncident toujours ici (un run écrit une seule ligne de
# calendrier, celle du jour du snapshot) : on expose la date uniquement
# via la partition et on omet la colonne `date` du schéma de données.
resource "aws_glue_catalog_table" "dim_date" {
  name          = "dim_date"
  database_name = aws_glue_catalog_database.soundpulse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification       = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.curated_bucket}/curated/dim_date/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "jour"
      type = "int"
    }
    columns {
      name = "semaine"
      type = "int"
    }
    columns {
      name = "mois"
      type = "int"
    }
    columns {
      name = "trimestre"
      type = "int"
    }
  }
}
