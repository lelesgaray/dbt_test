{%
set roles_table_name = 'roles' %}
    {%
set roles_table_name = source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + roles_table_name) %}

    {%
set users_table_name = 'users' %}
    {%
set users_table_name = source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + users_table_name) %}

    {%
set time_entries_table_name = 'time_entries' %}
    {%
set time_entries_table_name = source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + time_entries_table_name) %}

    {%
set time_entries_user_table_name = 'time_entries_user' %}
    {%
set time_entries_user_table_name = source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + time_entries_user_table_name) %}

    {%
set time_entries_client_table_name = 'time_entries_client' %}
    {%
set time_entries_client_table_name =
            source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + time_entries_client_table_name) %}

    {%
set time_entries_project_table_name = 'time_entries_project' %}
    {%
set time_entries_project_table_name =
            source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + time_entries_project_table_name) %}

    {%
set time_entries_task_table_name = 'time_entries_task' %}
    {%
set time_entries_task_table_name = source(var('DATASET_ID'), var('DEST_STREAM_PREFIX') + time_entries_task_table_name) %}
    WITH roles AS (SELECT roles_flattened, MAX(name) name
    FROM {{ roles_table_name }} roles
    CROSS JOIN UNNEST(roles.user_ids) roles_flattened
    GROUP BY 1)

SELECT spent_date                                  as date
     , users_2.name                                as name
     , users_2.id                                  as name_id
     , roles.name                                  as team_roles
     , users.roles                                 as team_users
     , users.email                                 as email
     , clients.name                                as client
     , projects.name                               as project
     , projects.id                                 as project_id
     , projects.code                               as project_code
     , CONCAT(projects.code, ' - ', projects.name) as project_full
     , tasks.name                                  as task
     , hours                                       as hours
     , billable                                    as billable
     , budgeted                                    as budgeted

FROM {{ time_entries_table_name }} times
         LEFT JOIN {{ time_entries_user_table_name }} users_2
ON times._airbyte_airbyte_harvest_time_entries_hashid =
    users_2._airbyte_airbyte_harvest_time_entries_hashid
    LEFT JOIN {{ time_entries_client_table_name }} clients
    ON times._airbyte_airbyte_harvest_time_entries_hashid =
    clients._airbyte_airbyte_harvest_time_entries_hashid
    LEFT JOIN {{ time_entries_project_table_name }} projects
    ON times._airbyte_airbyte_harvest_time_entries_hashid =
    projects._airbyte_airbyte_harvest_time_entries_hashid
    LEFT JOIN {{ time_entries_task_table_name }} tasks
    ON times._airbyte_airbyte_harvest_time_entries_hashid =
    tasks._airbyte_airbyte_harvest_time_entries_hashid
    LEFT JOIN {{ users_table_name }} users
    ON users_2.id = users.id
    LEFT JOIN roles
    ON CAST(users_2.id as STRING) = roles.roles_flattened
ORDER BY 1 ASC, 2 ASC