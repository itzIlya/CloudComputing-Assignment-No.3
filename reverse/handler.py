def handle(event, context):
    """
    Receives a string in the HTTP body and returns it reversed.
    """
    return event.body[::-1]
