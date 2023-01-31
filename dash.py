"""Runs a dashboard to visualise the build status of Bioconductor packages
"""
import time

from os.path import getmtime, isfile
from typing import Iterable, Optional
from warnings import simplefilter
from st_aggrid import AgGrid, GridOptionsBuilder, AgGridReturn, ColumnsAutoSizeMode
from st_aggrid.shared import GridUpdateMode

import streamlit as st
import altair as alt
import pandas as pd

from check import get_package_status, get_info

# ignore fufturewarning thrown by AgGrid
simplefilter("ignore", FutureWarning)


def aggrid_interactive_table(status_df: pd.DataFrame) -> AgGridReturn:
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        status_df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_side_bar()

    options.configure_selection("single")
    selection = AgGrid(
        status_df,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        gridOptions=options.build(),
        theme="streamlit",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )

    return selection


@st.cache
def get_dash_data(
    packages: Optional[Iterable[str]] = None, force: bool = False
) -> tuple[pd.DataFrame, float]:
    """Gets the data necessary to populate the dashboard.

    Args:
        packages (Optional[Iterable[str]], optional): List of packages of interest. Default is None.
        force (bool, optional): Whether to force recompute the data. Defaults to False.

    Returns:
        tuple[pd.DataFrame, float]: The dashboard data.
    """

    data_age = 0.0

    if force:
        status_df = get_package_status(packages=packages, devel=True)
        get_info(status_df)
    elif isfile("saved.pkl") and (data_age := (time.time() - getmtime("saved.pkl"))/3600) > 8:
        status_df = get_package_status(packages=packages, devel=True)
        get_info(status_df)
        pd.to_pickle(status_df, "saved.pkl")
    else:
        status_df = pd.read_pickle("saved.pkl")

    status_df = get_package_status(packages=packages, devel=True)
    get_info(status_df)
    pd.to_pickle(status_df, "saved.pkl")

    return status_df, data_age


def parse_input(user_input: str) -> list[str]:
    """Parses the user input.

    Args:
        user_input (str): The user input.

    Returns:
        list[str]: Parsed input.
    """

    input_list = user_input.strip().split(" ")

    return [x for x in input_list if x]


def run_dash():
    """Generates the dashboard.
    """

    st.write("""
    # Package Status Dashboard
    ## A little dashboard for monitoring your Bioconductor packages of interest.
    """)

    package_input = st.text_input(
        label="Type in some Bioconductor packages separated by \
               spaces (e.g, BiocCheck BiocGenerics S4Vectors).")

    packages = parse_input(package_input) if package_input else None

    # check if "fresh" data exists
    package_data, data_age = get_dash_data(
        packages=packages, force=bool(packages))
    data_age = round(data_age) if data_age else 0

    st.write(
        f"The data below are **{data_age} hour{'s' if data_age > 1 or not data_age else ''} old**.")

    fig = alt.Chart(package_data).mark_square(  # type: ignore
        size=500  # type: ignore
    ).encode(
        x="Name",
        y=alt.Y("Release:N", sort=("release", "devel")),  # type: ignore
        color=alt.Color(
            "Log Level",  # type: ignore
            sort=["OK", "WARNINGS", "ERROR", "NOT FOUND"],  # type: ignore
            scale=alt.Scale(  # type: ignore
                domain=["OK", "WARNINGS", "ERROR",
                        "NOT FOUND"],  # type: ignore
                range=["green", "orange", "red", "purple"])  # type: ignore
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
    selection = aggrid_interactive_table(
        status_df=package_data.sort_values(["Name"]))

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
            log_level = "warning" if (
                int(count) < 2 and "W" in log_level) else log_level.lower()
            st.write(f"### {name} had {count} {log_level} during {stage}.")

            for i, message in enumerate(messages):
                if not message:
                    continue
                st.write(f"**{log_level.capitalize().strip('s')} {i+1}**")
                st.code(message, language="r")


if __name__ == "__main__":
    run_dash()
