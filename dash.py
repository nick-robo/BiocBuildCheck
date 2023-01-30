
import time

from os.path import getmtime, isfile
from typing import Iterable, Optional
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

import streamlit as st
import altair as alt
import pandas as pd

from check import get_package_status, get_info


def aggrid_interactive_table(df: pd.DataFrame):
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_side_bar()

    options.configure_selection("single")
    selection = AgGrid(
        df,
        fit_columns_on_grid_load=True,
        gridOptions=options.build(),
        theme="streamlit",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )

    return selection

@st.cache
def get_dash_data(packages: Optional[Iterable[str]] = None, force: bool = False) -> tuple[pd.DataFrame, float]:

    data_age = 0.0

    if force:
        df = get_package_status(packages=packages, devel=True)
        get_info(df)
    elif isfile("saved.pkl") and (data_age := (time.time() - getmtime("saved.pkl"))/3600) > 8:
        df = get_package_status(packages=packages, devel=True)
        get_info(df)
        pd.to_pickle(df, "saved.pkl")
    else:
        df = pd.read_pickle("saved.pkl")

    df = get_package_status(packages=packages, devel=True)
    get_info(df)
    pd.to_pickle(df, "saved.pkl")

    return df, data_age


def parse_input(user_input: str) -> list[str]:

    input_list = user_input.split(" ")

    filtered_list = filter(lambda x: not (" " in x or not ""), input_list)
    return list(input_list)


st.write("""
# Package Status Dashboard
## A little dashboard for monitoring your Bioconductor packages of interest.
""")

package_input = st.text_input(
    label="Type in some Bioconductor packages separated by spaces (e.g, BiocCheck BiocGenerics S4Vectors).")


packages = parse_input(package_input) if package_input else None

# check if "fresh" data exists
package_data, data_age = get_dash_data(packages=packages, force=True if packages else False)
data_age = round(data_age) if data_age else 0

st.write(
    f"The data below are **{data_age} hour{'s' if data_age > 1 or not data_age else ''} old**.")

fig = alt.Chart(package_data).mark_square(
    size=500
).encode(
    x="Name",
    y=alt.Y("Release:N", sort=("release", "devel")),
    color=alt.Color(
        "Log Level",
        sort=["OK", "WARNINGS", "ERROR", "NOT FOUND"],
        scale=alt.Scale(
            domain=["OK", "WARNINGS", "ERROR", "NOT FOUND"],
            range=["green", "orange", "red", "purple"])
    )
).configure_axis(
    labelFontSize=18
).properties(
    height=250
)


# alt.Chart(df).mark_bar().encode(  # type: ignore
#     x="Name", y="Message Count",
#     color=alt.Color("Log Level:O", scale=alt.Scale(  # type: ignore
#         scheme="dark2"))  # type: ignore
# )
st.altair_chart(fig, use_container_width=True)


st.write("Click on a row to view the message details.")
selection = aggrid_interactive_table(df=package_data.sort_values(["Name"]))

if selection.selected_rows:
    _, name, release, log_level, stage, count, * \
        messages = selection.selected_rows[0].values()
    if log_level == "OK":
        st.write(
            f"### There were no problems in the *{release}* build of **{name}**.")
    elif log_level == "NOT FOUND":
        st.write(
            f"### **{name}** was not found in the list of Bioconductor packages.")

    else:
        # change warnings to warning if message count is smaller than 2
        log_level = "warning" if count < 2 and "W" in log_level else log_level.lower()
        st.write(f"### {name} had {count} {log_level} during {stage}.")

        for i, message in enumerate(messages):
            if not message:
                continue
            st.write(f"**{log_level.capitalize()} {i+1}**")
            st.code(message, language="r")

# %%
