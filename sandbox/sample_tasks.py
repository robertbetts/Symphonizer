
parse_swift = {
    "service": "swift_processor",
    "method": "parse",
    "params": {
        "raw_swift": None
    }
}

enrich_swift = {
    "service": "swift_processor",
    "method": "enrich",
    "params": {
        "swift_message": None
    }
}

notify_client = {
    "service": "client_docs",
    "method": "generate_doc",
    "params": {
        "template": "ca_notify",
        "corp_action_data": None
    }
}

book_corporate_action = {
    "service": "ledger",
    "method": "book_corporate_action",
    "params": {
        "corp_action_data": None
    }
}

available_tasks = {parse_swift, enrich_swift, notify_client, book_corporate_action}
