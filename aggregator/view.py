"""
Module Name: view.py
Created: 2022-08-08
Creator: JL
Change Log: 2022-08-08 - initial commit
Summary: view displays the output of any find requests
Functions: display_results
"""
from aggregator.model import JavaLog


async def display_result(result: list[JavaLog] | None) -> None:
    headers = (
        "ObjectId\t\t", "Node", "Severity", "JVM",
        "Timestamp", "Source", "Type", "Message"
    )
    out = "| "
    for head in headers:
        out = f"{out}{head}\t| "

    out = out.strip()
    out = out + "\n"

    results = []
    if isinstance(result, JavaLog):
        results.append(result)
    else:
        results = result
    for result in results:
        id = result.id
        node = result.node
        severity = result.severity
        jvm = result.jvm
        timestamp = result.datetime
        source = result.source
        type = result.type
        message = result.message
        out = (
            f"{out}| {id}\t| {node}\t| {severity}\t| {jvm}\t| "
            f"{timestamp}\t| {source}\t| {type}\t| {message}\t|\n"
        )

    print(out)
