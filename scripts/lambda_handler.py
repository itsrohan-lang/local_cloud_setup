def handler(event, context):
    print("Received event:", event)
    return {"status": "SUCCESS", "message": "Lambda executed locally!"}
