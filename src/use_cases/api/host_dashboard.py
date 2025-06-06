import enum
import json
from dataclasses import fields

from src.models.node import EquivalentId
from src.use_cases.build_from_yaml import HostDashboardFromYaml
import streamlit as st
st.set_page_config(layout="wide")


def safe_serialize(value):
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, list):
        return [safe_serialize(item) for item in value]
    if isinstance(value, EquivalentId):
        return value.id_str()
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    try:
        return json.dumps(value, default=str)  # fallback to str if not serializable
    except Exception:
        return str(value)


def get_filter(st, data_model):
    filters = {}
    for key, value in st.session_state.items():
        if value and '|' in key:  # Ensure it's a checkbox key
            api_label, filter_model, field, actual_value = key.split('|')
            if api_label != api.label:
                continue
            if filter_model != data_model:
                continue
            if field not in filters:
                filters[field] = []
            if actual_value == 'True':
                filters[field].append(True)
            elif actual_value == 'False':
                filters[field].append(False)
            elif actual_value == 'None':
                filters[field].append(None)
            else:
                filters[field].append(actual_value)
    return filters


yaml_options = {
    "Pharos PROD": "./src/use_cases/api/pharos_prod_dashboard.yaml",
    "Pharos DEV": "./src/use_cases/api/pharos_dev_dashboard.yaml",
    "Pounce PROD": "./src/use_cases/api/pounce_prod_dashboard.yaml",
    "Pounce DEV": "./src/use_cases/api/pounce_dev_dashboard.yaml"
}

selected_label = st.selectbox("Choose a data source", options=list(yaml_options.keys()))
yaml_file = yaml_options[selected_label]

dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
config = dashboard.configuration.config_dict['dashboard']
api = dashboard.api_adapter

st.title(f"Data Browser: {api.label}")

if api.credentials.internal_url != api.credentials.url:
    st.write(f"{api.credentials.url} ({api.credentials.internal_url})")
else:
    st.write(api.credentials.url)


st.markdown(f"<a href='./schema?api={yaml_file}' target='_blank'>View Schema</a>", unsafe_allow_html=True)


edge_names = api.list_edges()
node_names = api.list_nodes()

view_type = st.radio("Choose model type", ["Nodes", "Edges"], horizontal=True)
model_names = node_names if view_type == "Nodes" else edge_names

count_map = {}
for model_name in model_names:
    result = api.get_count(model_name, get_filter(st, model_name))
    count_map[model_name] = result.count

model_names = sorted(
    model_names,
    key=lambda x: (
        config['tab_order'].index(x) if x in config['tab_order'] else float('inf')
    )
)

tabs = st.container()
with tabs:
    st.markdown(
        """
        <style>
        .stTabs [data-baseweb="tab-list"] {
            display: flex;
            justify-content: space-between;
            width: 100%;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    selected_tab = st.tabs(model_names)
    for i, t in enumerate(selected_tab):
        with t:
            col_left, col_right = st.columns([1, 3])  # Adjust
            with col_left:
                configured_facets = [
                    model.get('facets', [])
                    for model in config['models']
                    if model['class'] == model_names[i]
                ]
                if len(configured_facets) > 0:
                    configured_facets = configured_facets[0]

                if len(configured_facets) == 0:
                    st.write("No facets configured for this model")
                else:
                    facet_list = configured_facets
                    for facet in facet_list:
                        st.markdown(f"<div style='font-size: 2em;'>{facet}</div>", unsafe_allow_html=True)
                        facet_had_data = False
                        with st.expander(facet, expanded=True):
                            results = api.get_facet_values(model_names[i], facet, filter=get_filter(st, model_names[i]))
                            facet_results = results.facet_values
                            if facet_results:
                                facet_had_data = True
                                for result in facet_results:
                                    col1, col2, col3 = st.columns([2, 1, 1])
                                    col1.write(f"{result['value']}")
                                    col2.write(result['count'])
                                    key = f"{api.label}|{model_names[i]}|{facet}|{result['value']}"
                                    result['selected'] = col3.checkbox("Filter",
                                                                      key=key,
                                                                      label_visibility="collapsed")
                            else:
                                st.write("No data available for this facet.")
                        # if facet_had_data:
                        #     with st.expander("Show Query", expanded=False):
                        #         st.code(results.query, language='aql')
            with col_right:
                total_count = count_map[model_names[i]]

                st.markdown(f"<div style='font-size: 2em;'>Total Count: {total_count}</div>", unsafe_allow_html=True)

                filters = get_filter(st, model_names[i])

                st.write("Selected filters:")
                if not filters:
                    st.write("None")
                else:
                    for key, values in filters.items():
                        st.write(f"{key}: {', '.join([str(val) for val in values])}")

                # Initialize skip in session state if not already set
                if f"skip_{api.label}_{model_names[i]}" not in st.session_state:
                    st.session_state[f"skip_{api.label}_{model_names[i]}"] = 0

                page_size = 10

                # Add navigation buttons
                col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
                with col1:
                    if st.button("First Page", key=f"first_{model_names[i]}"):
                        st.session_state[f"skip_{api.label}_{model_names[i]}"] = 0
                with col2:
                    if st.button("Previous", key=f"prev_{model_names[i]}") and st.session_state[f"skip_{api.label}_{model_names[i]}"] > 0:
                        st.session_state[f"skip_{api.label}_{model_names[i]}"] -= page_size  # Adjust page size as needed
                with col4:
                    if st.button("Next", key=f"next_{model_names[i]}"):
                        st.session_state[f"skip_{api.label}_{model_names[i]}"] += page_size  # Adjust page size as needed
                with col5:
                    if st.button("Last Page", key=f"last_{model_names[i]}"):
                        st.session_state[f"skip_{api.label}_{model_names[i]}"] = (total_count // page_size) * page_size  # Adjust page size as needed

                # Display the current range being shown
                start_range = st.session_state[f"skip_{api.label}_{model_names[i]}"] + 1
                end_range = min(st.session_state[f"skip_{api.label}_{model_names[i]}"] + page_size, total_count)
                st.write(f"Showing items {start_range} to {end_range} of {total_count}")

                # Fetch and display the data
                result = api.get_list(model_names[i], get_filter(st, model_names[i]), top=page_size, skip=st.session_state[f"skip_{api.label}_{model_names[i]}"])
                data_list = result.list

                if hasattr(data_list[0], 'id'):
                    st.markdown(f"<a href='./details?model={model_names[i]}&id={data_list[0].id}&api={yaml_file}' target='_blank'>Go to Details</a>", unsafe_allow_html=True)
                # if 'start_id' in data_list[0]:
                #     st.markdown(f"<a href='./details?model={model_names[i]}&id={data_list[0]['start_id']}&api={yaml_file}' target='_blank'>Go to Details</a>", unsafe_allow_html=True)
                # if 'end_id' in data_list[0]:
                #     st.markdown(f"<a href='./details?model={model_names[i]}&id={data_list[0]['end_id']}&api={yaml_file}' target='_blank'>Go to Details</a>", unsafe_allow_html=True)


                configured_fields = [
                    model.get('column_order', [])
                    for model in config['models']
                    if model['class'] == model_names[i]
                ]
                if len(configured_fields) > 0:
                    configured_fields = configured_fields[0]

                if view_type == 'Nodes':
                    configured_fields = ['id'] + configured_fields
                else:
                    configured_fields = ['start_node', 'end_node'] + configured_fields

                all_keys = set(field.name for item in data_list for field in fields(type(item)))
                final_fields = configured_fields + [key for key in all_keys if key not in configured_fields]

                serialized_data_list = [
                    {key: safe_serialize(getattr(item, key)) for key in final_fields if hasattr(item, key)}
                    for item in data_list
                ]

                if serialized_data_list:
                    st.dataframe(serialized_data_list)
                else:
                    st.write("No data available for this model.")

                st.code(result.query, language='aql')
