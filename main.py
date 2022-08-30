from UserSearchClient import UsersSearchClient

if __name__ == '__main__':
    client = UsersSearchClient()
    # client = UsersSearchClient(debug=True)
    users = client.GetUsers(limit=10, with_digest=True)
    users = client.GetUsers(skip=10, limit=40, with_digest=False)
    users = client.GetUsers(skip=40, limit=87, fields=["first_name", "last_name", "digestive_value"], with_digest=True)
    users = client.GetUsers(skip=92, limit=142, fields=["first_name", "last_name", "digestive_value"], with_digest=True)
    all_users = client.GetUsers()
    for user in all_users:
        print(user.GetField("digestive_value"))




