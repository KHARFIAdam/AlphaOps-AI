import re
from langchain_community.utilities import SQLDatabase
from langchain_mistralai import ChatMistralAI
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph, END
from typing import TypedDict, Annotated
import operator
from sql_validator import SQLValidator
import os
from dotenv import load_dotenv
import re

load_dotenv()
print("MISTRAL_API_KEY loaded?", bool(os.getenv("MISTRAL_API_KEY")))

db = SQLDatabase.from_uri("postgresql+psycopg2://nl2sql_user:machinelearning@localhost:5432/nl2sql_db")

llm = ChatMistralAI(model="codestral-latest", api_key=os.getenv("MISTRAL_API_KEY"), temperature=0.1)

class AgentState(TypedDict):
    input: str
    sql_query: str
    validation: dict
    result: str
    messages: Annotated[list, operator.add]
    db_schema: str

def get_schema(state):
    schema = db.get_table_info(["dim_tickers", "dimtime", "fact_ohlcv"])
    return {"db_schema": schema}

def generate_sql(state):
    prompt = ChatPromptTemplate.from_template("""
Schéma DB stocks :
{db_schema}

Question user : {input}

Génère UNE SEULE requête SELECT PostgreSQL safe. Utilise joins dim_tickers, dim_time, fact_ohlcv. Ne réponds qu'avec la requête SQL, sans explication.
De plus, Si la date demandée est un jour non-tradé, utiliser la dernière date disponible avant cette date (MAX(date) <= target_date).""")

    chain = prompt | llm

    sql = chain.invoke({
        "db_schema": state["db_schema"],
        "input": state["input"],
    }).content.strip()

    # enlever ```sql ... ```
    sql = re.sub(r"^```sql\s*|^```\s*|```$", "", sql, flags=re.IGNORECASE | re.MULTILINE).strip()

    return {"sql_query": sql}

def validate_sql(state):
    validator = SQLValidator()
    return {"validation": validator.validate(state['sql_query'])}


def execute_sql(state):
    if not state.get('validation', {}).get('is_valid'):
        return {"result": state['validation']['reason']}
    try:
        result = db.run(state['sql_query'])
        return {"result": result}
    except Exception as e:
        return {"result": f"Sheesh execution error: {str(e)}"}
    

workflow = StateGraph(AgentState)
workflow.add_node("get_schema", get_schema)
workflow.add_node("generate_sql", generate_sql)
workflow.add_node("validate_sql", validate_sql)
workflow.add_node("execute_sql", execute_sql)

workflow.set_entry_point("get_schema")
workflow.add_edge("get_schema", "generate_sql")
workflow.add_edge("generate_sql", "validate_sql")
workflow.add_conditional_edges(
    "validate_sql",
    lambda s: "execute_sql" if s['validation']['is_valid'] else END
)
workflow.add_edge("execute_sql", END)


app = workflow.compile()

result = app.invoke({"input": "Prix NVDA hier ?"})
print("SQL généré:", result.get('sql_query', 'No SQL'))
print("Résultat pour Prix NVDA hier ?:", result.get("result", result.get("validation", "No result")))