{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

  {% block methods %}
  {% if methods %}
  .. rubric:: {{ _('Methods') }}

  .. autosummary::
    :toctree: ./{{ module }}/{{ name }}
    {% for item in methods %}
      {%- if item not in ['__init__'] %}
        ~{{ name }}.{{ item }}
      {%- endif -%}
    {%- endfor %}
    {% endif %}
    {% endblock %}

  {% block attributes %}
  {% if attributes %}

  .. rubric:: {{ _('Attributes') }}

  .. autosummary::
    :toctree: ./{{ module }}/{{ name }}
      {% for item in attributes %}
        {%- if not item.startswith('_') %}
          ~{{ name }}.{{ item }}
        {%- endif -%}
      {%- endfor %}
  {% endif %}
  {% endblock %}
