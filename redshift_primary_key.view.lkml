view: redshift_primary_key {


    derived_table: {
      sql:

      SELECT
             (current_database())::information_schema.sql_identifier AS table_catalog,
             (nr.nspname)::information_schema.sql_identifier AS table_schema,
             (r.relname)::information_schema.sql_identifier AS table_name,
             (a.attname)::information_schema.sql_identifier AS column_name,
             (pos.n)::information_schema.cardinal_number AS ordinal_position,
            CASE WHEN (c.contype = 'c'::"char") THEN 'CHECK'::text
                 WHEN (c.contype = 'f'::"char") THEN 'FOREIGN KEY'::text
                 WHEN (c.contype = 'p'::"char") THEN 'PRIMARY KEY'::text
                 WHEN (c.contype = 'u'::"char") THEN 'UNIQUE'::text ELSE NULL::text END AS constraint_type
                FROM pg_namespace nr, pg_class r, pg_attribute a, pg_constraint c, pg_user u,
                     information_schema._pg_keypositions() pos(n)
                WHERE nr.oid = r.relnamespace
                  AND r.oid = a.attrelid
                  AND r.oid = c.conrelid
                  AND NOT a.attisdropped
                  AND (c.conkey[pos.n] = a.attnum)
                  AND r.relkind = 'r'::"char"
                  AND r.relowner = u.usesysid
                 -- AND constraint_type='PRIMARY KEY'
                 -- and table_schema = 'dwh_il'
                order by table_name, ordinal_position
                ;;
    }
    # dimensions #

    # Identifiers {
    dimension: table_catalog {
      type:  string
      sql: ${TABLE}.table_catalog  ;;
    }
  dimension: table_name {
    type:  string
    sql: ${TABLE}.table_name  ;;
  }

    dimension: table_schema {
      type:  string
      sql: ${TABLE}.table_schema  ;;
    }

    dimension: column_name{
      type:  string
      sql: ${TABLE}.column_name  ;;
    }

    dimension: ordinal_position{
      type:  string
      sql: ${TABLE}.ordinal_position  ;;
    }

  dimension: constraint_type{
    type:  string
    sql: ${TABLE}.constraint_type ;;
  }
}

  view: redshift_tables_list {


    derived_table: {
      sql:

      SELECT
             (current_database())::information_schema.sql_identifier AS table_catalog,
             (nr.nspname)::information_schema.sql_identifier AS table_schema,
             (r.relname)::information_schema.sql_identifier AS table_name
           FROM pg_namespace nr, pg_class r, pg_user u
                WHERE nr.oid = r.relnamespace
                  AND r.relkind = 'r'::"char"
                  AND r.relowner = u.usesysid
                group by 1,2,3
                order by table_name;;
             }
      dimension: table_catlog{
        type:  string
        sql: ${TABLE}.table_catalog ;;
      }
      dimension: table_schema{
        type:  string
        sql: ${TABLE}.table_schema ;;
      }
      dimension: table_name{
        type:  string
        sql: ${TABLE}.table_name ;;
      }





  # # You can specify the table name if it's different from the view name:
  # sql_table_name: my_schema_name.tester ;;
  #
  # # Define your dimensions and measures here, like this:
  # dimension: user_id {
  #   description: "Unique ID for each user that has ordered"
  #   type: number
  #   sql: ${TABLE}.user_id ;;
  # }
  #
  # dimension: lifetime_orders {
  #   description: "The total number of orders for each user"
  #   type: number
  #   sql: ${TABLE}.lifetime_orders ;;
  # }
  #
  # dimension_group: most_recent_purchase {
  #   description: "The date when each user last ordered"
  #   type: time
  #   timeframes: [date, week, month, year]
  #   sql: ${TABLE}.most_recent_purchase_at ;;
  # }
  #
  # measure: total_lifetime_orders {
  #   description: "Use this for counting lifetime orders across many users"
  #   type: sum
  #   sql: ${lifetime_orders} ;;
  # }
}

# view: redshift_primary_key {
#   # Or, you could make this view a derived table, like this:
#   derived_table: {
#     sql: SELECT
#         user_id as user_id
#         , COUNT(*) as lifetime_orders
#         , MAX(orders.created_at) as most_recent_purchase_at
#       FROM orders
#       GROUP BY user_id
#       ;;
#   }
#
#   # Define your dimensions and measures here, like this:
#   dimension: user_id {
#     description: "Unique ID for each user that has ordered"
#     type: number
#     sql: ${TABLE}.user_id ;;
#   }
#
#   dimension: lifetime_orders {
#     description: "The total number of orders for each user"
#     type: number
#     sql: ${TABLE}.lifetime_orders ;;
#   }
#
#   dimension_group: most_recent_purchase {
#     description: "The date when each user last ordered"
#     type: time
#     timeframes: [date, week, month, year]
#     sql: ${TABLE}.most_recent_purchase_at ;;
#   }
#
#   measure: total_lifetime_orders {
#     description: "Use this for counting lifetime orders across many users"
#     type: sum
#     sql: ${lifetime_orders} ;;
#   }
# }
