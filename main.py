from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from google.cloud import bigquery
import plotly.graph_objects as go
import pandas as pd
import os

app = FastAPI()

credentials_file_path = "/Users/##########/##############"  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file_path

project_id = "####"  
bigquery_client = bigquery.Client()

@app.get("/display_sankey", response_class=HTMLResponse)
async def display_sankey_chart():
    """
    Fetches data from BigQuery and renders a Sankey chart in the browser.
    """
    try:
        # Define the query to generate Sankey data
        query = """ WITH ScreenData AS (
    SELECT 
        territory,
        `Session Number`,
        `Current Screen`,
        PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', Timestamp) AS parsed_timestamp
    FROM 
        `odin-backup-432014.onehub.onehub_analytics`
),
WithLead AS (
    SELECT 
        territory,
        `Session Number`,
        `Current Screen` AS source,
        LEAD(`Current Screen`) OVER (PARTITION BY territory, `Session Number` ORDER BY parsed_timestamp) AS target
    FROM 
        ScreenData
),
FilteredTransitions AS (
    SELECT 
        source,
        target
    FROM 
        WithLead
    WHERE 
        target IS NOT NULL
        AND source != target
),
TopTransitions AS (
    SELECT 
        source,
        target,
        COUNT(*) AS transition_count
    FROM 
        FilteredTransitions
    GROUP BY 
        source, target
    ORDER BY 
        transition_count DESC
    LIMIT 20 
)
SELECT 
    source AS Source,
    target AS Target,
    transition_count AS Value
FROM 
    TopTransitions;

"""
        
        # Run the query and convert to DataFrame
        query_job = bigquery_client.query(query)
        df = query_job.result().to_dataframe()

        # Ensure the DataFrame has required columns
        if not {"Source", "Target", "Value"}.issubset(df.columns):
            raise ValueError("Query result must contain 'Source', 'Target', and 'Value' columns.")

        # Create Sankey chart
        all_nodes = list(set(df["Source"]).union(set(df["Target"])))
        node_indices = {node: idx for idx, node in enumerate(all_nodes)}
        source_indices = df["Source"].map(node_indices)
        target_indices = df["Target"].map(node_indices)

        # Generate random colors for each link
        import random
        link_colors = [f"rgba({random.randint(0,255)},{random.randint(0,255)},{random.randint(0,255)},0.5)" for _ in range(len(df))]

        fig = go.Figure(go.Sankey(
            node=dict(
                pad=30,
                thickness=25,
                line=dict(color="blue", width=2),
                label=all_nodes,
            ),
            link=dict(
                source=source_indices,
                target=target_indices,
                value=df["Value"],
                color=link_colors  # Assigning colors to links
            )
        ))

        # Update layout for better clarity
        fig.update_layout(
            title_text="Enhanced User Flow Sankey Diagram",
            title_font_size=20,
            font=dict(size=15),
            margin=dict(l=75, r=100, t=220, b=200), # (left, right, top, bottom)
            height=830,
            width=1600
        )
        chart_html = fig.to_html(full_html=False)

        # Return the chart as HTML content
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Sankey Chart</title>
        </head>
        <body>
            {chart_html}    
        </body>
        </html>
        """

    except Exception as e:
        return HTMLResponse(content=f"<h1>Error: {str(e)}</h1>", status_code=500)



@app.get("/from_source/{source_node}", response_class=HTMLResponse)
async def display_sankey_from_source(source_node: str):
    """
    Generate a Sankey diagram starting from a specific source node.

    Path Parameter:
    - source_node: The node (current screen) to use as the source in the Sankey chart.

    Returns:
    - HTML content with the generated Sankey diagram.
    """
    try:
        # BigQuery query with parameterized input
        query = """
            WITH ScreenData AS (
                SELECT 
                    territory,
                    `Session Number`,
                    `Current Screen`,
                    PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', Timestamp) AS parsed_timestamp
                FROM 
                    `odin-backup-432014.onehub.onehub_analytics`
            ),
            WithLead AS (
                SELECT 
                    territory,
                    `Session Number`,
                    `Current Screen` AS source,
                    LEAD(`Current Screen`) OVER (PARTITION BY territory, `Session Number` ORDER BY parsed_timestamp) AS target
                FROM 
                    ScreenData
            ),
            FilteredTransitions AS (
                SELECT 
                    source,
                    target
                FROM 
                    WithLead
                WHERE 
                    target IS NOT NULL
                    AND source = @source_input
            ),
            AggregatedTransitions AS (
                SELECT 
                    source,
                    target,
                    COUNT(*) AS transition_count
                FROM 
                    FilteredTransitions
                GROUP BY 
                    source, target
                ORDER BY 
                    transition_count DESC
            )
            SELECT 
                source AS Source,
                target AS Target,
                transition_count AS Value
            FROM 
                AggregatedTransitions;
        """

        # Pass the source_node as a parameter to the query
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source_input", "STRING", source_node)
            ]
        )
        query_job = bigquery_client.query(query, job_config=job_config)
        df = query_job.result().to_dataframe()

        # Map unique nodes to indices
        all_nodes = list(set(df["Source"]).union(set(df["Target"])))
        node_indices = {node: idx for idx, node in enumerate(all_nodes)}
        df["source_idx"] = df["Source"].map(node_indices)
        df["target_idx"] = df["Target"].map(node_indices)

        # Create the Sankey diagram
        fig = go.Figure(go.Sankey(
            node=dict(
                pad=15,
                thickness=15,
                line=dict(color="black", width=0.5),
                label=all_nodes,
                color="lightblue"
            ),
            link=dict(
                source=df["source_idx"],
                target=df["target_idx"],
                value=df["Value"],
                color="rgba(0,100,255,0.6)"
            )
        ))

        # Layout adjustments
        fig.update_layout(
            title_text=f"Sankey Diagram from Source: {source_node}",
            title_font_size=24,
            height=600,
            width=1000
        )

        # Return chart as HTML
        return fig.to_html(full_html=False)

    except Exception as e:
        return HTMLResponse(content=f"<h1>Error: {str(e)}</h1>", status_code=500)


