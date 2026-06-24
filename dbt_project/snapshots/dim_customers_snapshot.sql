{% snapshot dim_customers_snapshot %}

{# ──────────────────────────────────────────────────────────────────────────
 # SCD Type 2 history for dim_customers.
 #
 # Tracks changes to demographic attributes that the business cares about
 # historically: country (relocations), marital_status (life events),
 # gender (rare but tracked).
 #
 # Why customer_id (not customer_key) as unique_key:
 #   customer_key is a row_number() surrogate that gets renumbered every
 #   time dim_customers is rebuilt. snapshotting on it would flag every
 #   customer as "changed" on every run. customer_id is the stable
 #   natural key from CRM.
 #
 # Why strategy='check' (not 'timestamp'):
 #   dim_customers has no reliable "last modified" timestamp from the
 #   source systems, so we let dbt compute a hash of the monitored
 #   columns each run and compare to the previous snapshot.
 #
 # Why NOT snapshot first_name / last_name / birthdate / create_date:
 #   They effectively never change in this domain. Adding them would
 #   only increase snapshot table size without business value.
 # ──────────────────────────────────────────────────────────────────────── #}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['country', 'marital_status', 'gender'],
      invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    customer_number,
    first_name,
    last_name,
    birthdate,
    country,
    marital_status,
    gender,
    create_date
from {{ ref('dim_customers') }}

{% endsnapshot %}
