{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run=True) %}
    
    {% set get_drop_commands_query %}
        select
            case 
                when table_type = 'VIEW' then table_type
            else
                'TABLE'
            end as drop_type, 
            'DROP ' || case when table_type = 'VIEW' then table_type else 'TABLE' end || ' {{ database }}.' || table_schema || '.' || table_name || ';'
        from {{ database }}.{{ schema }}.INFORMATION_SCHEMA.TABLES
        where table_schema = ('{{ schema }}')
        and date(creation_time) <= date_sub(current_date(), interval {{ days }} day)
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}
    
{% endmacro %}