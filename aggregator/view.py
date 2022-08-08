"""
Module Name: view.py
Created: 2022-08-08
Creator: JL
Change Log: 2022-08-08 - initial commit
Summary: view displays the output of any find requests
Functions: display_results
"""
from aggregator.model import JavaLog


async def display_result(results: list[JavaLog] | None) -> None:
    headers = (
        "ObjectId\t\t", "Node", "Severity", "JVM",
        "Timestamp", "Source", "Type", "Message"
    )
    out = "| "
    for head in headers:
        out = f"{out}{head}\t| "

    out = out.strip()
    out = out + "\n"

    id = results.id
    node = results.node
    severity = results.severity
    jvm = results.jvm
    timestamp = results.datetime
    source = results.source
    type = results.type
    message = results.message
    out = (
        f"{out}| {id}\t| {node}\t| {severity}\t| {jvm}\t| "
        f"{timestamp}\t| {source}\t| {type}\t| {message}\t|"
    )

    print(out)
