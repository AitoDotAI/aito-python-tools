{{ fullname | escape | underline}}

.. automodule:: {{ fullname }}

  {% block attributes %}
  {% if attributes %}
  .. rubric:: Module Attributes

  .. autosummary::
    :toctree: ./{{ fullname }}
    {% for item in attributes %}
      {{ item }}
    {%- endfor %}
  {% endif %}
  {% endblock %}

  {% block functions %}
  {% if functions %}
  .. rubric:: {{ _('Functions') }}

  .. autosummary::
    :toctree: ./{{ fullname }}
    {% for item in functions %}
      {{ item }}
    {%- endfor %}
  {% endif %}
  {% endblock %}

  {% block classes %}
  {% if classes %}
  .. rubric:: {{ _('Classes') }}

  .. autosummary::
    :toctree: ./{{ fullname }}
    {% for item in classes %}
      {{ item }}
    {%- endfor %}
  {% endif %}
  {% endblock %}

  {% block exceptions %}
  {% if exceptions %}
  .. rubric:: {{ _('Exceptions') }}

  .. autosummary::
    :toctree: ./{{ fullname }}
    {% for item in exceptions %}
      {{ item }}
    {%- endfor %}
  {% endif %}
  {% endblock %}
