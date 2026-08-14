{#
    The relation name is derived from the model name -- never hand-written.

      stg_<module>__<subject>  ->  staging.<module>__<subject>
      int_<module>__<subject>  ->  intermediate.<module>__<subject>
      <module>__<subject>      ->  <layer>.<subject>

    Internal models keep the module token because staging/ and intermediate/ are
    shared across every module; published models drop it because their schema
    already names the module.

    No exceptions. If a model needs a relation name this macro would not produce,
    the model name is wrong.
#}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}

    {%- elif node is none -%}

    {%- elif node.resource_type != 'model' or node.package_name != 'the_tuva_project' -%}
        {{ node.name }}

    {%- elif node.name.startswith('stg_') or node.name.startswith('int_') -%}
        {{ node.name[4:] }}

    {%- else -%}
        {{ node.name.split('__', 1)[1] }}

    {%- endif -%}

{%- endmacro %}
