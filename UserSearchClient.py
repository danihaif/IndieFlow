# system imports
import yaml
import requests
import itertools
import math
import time
from threading import Thread

# project imports
from UserSearchExceptions import InvalidFieldException, Colors
from UserSearchConsts import allowed_fields


class UsersSearchClient:

    def __init__(self, debug=False):
        self._debug = debug
        with open('configuration.yaml', 'r') as configuration:
            data = yaml.load(configuration, Loader=yaml.FullLoader)
            self._mocki_api_url = data['endpoints']['mocki']
            self._hashify_api_url = data['endpoints']['hashify']
            self._users = {}
            try:
                users_json = requests.get(url=self._mocki_api_url).json()
            except Exception as exc:
                print(Colors.WARNING + f"Error retrieving data from endpoint. Exception: {exc}" + Colors.ENDC)
                return
            for user_entry in users_json:
                user = User(user_id=user_entry['_id'], index=user_entry['index'],
                            guid=user_entry['guid'], is_active=user_entry['isActive'],
                            balance=user_entry['balance'], picture=user_entry['picture'],
                            age=user_entry['age'], eye_color=user_entry['eyeColor'],
                            first_name=user_entry['name']['first'], last_name=user_entry['name']['last'],
                            company=user_entry['company'], email=user_entry['email'],
                            phone=user_entry['phone'], address=user_entry['address'],
                            about=user_entry['about'], registered=user_entry['registered'],
                            latitude=user_entry['latitude'], longitude=user_entry['longitude'],
                            tags=user_entry['tags'], user_range=user_entry['range'],
                            friends=user_entry['friends'], greeting=user_entry['greeting'],
                            favorite_fruit=user_entry['favoriteFruit'])
                self._users[user_entry['_id']] = user

    def GetUser(self, user_id, with_digest=False):
        if not self._users:
            print(Colors.WARNING + "Client was not initialized correctly" + Colors.ENDC)
            return
        try:
            if with_digest:
                self.__GenerateDigestiveValueForUser(self._users[user_id])
            return self._users[user_id]
        except KeyError:
            print(Colors.WARNING + "KeyError: Invalid user id" + Colors.ENDC)

    def GetUsers(self, skip=0, limit=-math.inf, fields=None, with_digest=False):
        if not self._users:
            print(Colors.WARNING + "Client was not initialized correctly" + Colors.ENDC)
            return

        if limit == -math.inf:
            limit = self._users.__len__() + 1
        try:
            result_users = list(itertools.islice(self._users.values(), skip, limit))
        except ValueError:
            print(Colors.WARNING + """Indices for skip and limit must be None or an integer: \
0 <= x <= sys.maxsize. Retrieving all users instead.""" + Colors.ENDC)
            result_users = list(self._users.values())

        if with_digest:
            start = time.time()
            self.__GenerateDigestiveValueMultiThreaded(result_users, skip)
            end = time.time()
            self.__DebugPrint(f"Generating digestive value for users took {end-start} seconds")

        if fields is not None:
            try:
                for field in fields:
                    if field not in allowed_fields:
                        raise InvalidFieldException(field)
            except InvalidFieldException as exc:
                print(Colors.WARNING + f"InvalidFieldException: '" + exc.__str__() + "' is not allowed" + Colors.ENDC)
                print(Colors.WARNING + f"Allowed fields are {allowed_fields}" + Colors.ENDC)
                return

            result_users_with_fields = []
            for user in result_users:
                user_with_fields = {}
                for field in fields:
                    user_with_fields[field] = user.GetField(field)
                result_users_with_fields.append(user_with_fields)
            return result_users_with_fields
        else:
            return result_users

    def __GenerateDigestiveValueMultiThreaded(self, result_users, skip):
        # Let's split the result_users array to even parts and spawn threads to generate digestive value concurrently

        result_users_length = result_users.__len__()
        # We can't have more than 10 concurrent requests
        num_of_threads = min(int(result_users_length / 10) + 1, 10)
        step = math.floor(result_users_length / num_of_threads)
        threads = []
        first_index = 0
        last_index = step
        for i in range(num_of_threads):
            if i == num_of_threads - 1:
                # this is the last round, lets make sure we are getting all of the users
                last_index = result_users_length

            self.__DebugPrint(f"first_index: {first_index + skip} last index: {last_index + skip} ")
            thread = Thread(target=self.__GenerateDigestiveValueForUsers,
                            args=(list(self._users.values())[first_index + skip:last_index + skip],))
            threads.append(thread)
            thread.start()
            first_index = last_index
            last_index += step
            last_index = min(last_index, result_users_length)
        for thread in threads:
            thread.join()

    def __GenerateDigestiveValueForUser(self, user):
        if user.GetField("digestive_value") is None:
            first_name = user.GetField("first_name")
            last_name = user.GetField("last_name")
            hashify_url = self._hashify_api_url.replace("FirstLastName", f"{first_name}{last_name}")
            try:
                digestive_value_json = requests.get(url=hashify_url).json()
                digest = digestive_value_json['Digest']
                user._digestive_value = digest
            except Exception as exc:
                print(Colors.WARNING + f"Error retrieving data from endpoint. Exception: {exc}" + Colors.ENDC)
                return

    def __GenerateDigestiveValueForUsers(self, users):
        for user in users:
            self.__GenerateDigestiveValueForUser(user)

    def __DebugPrint(self, debug_msg):
        if self._debug:
            print(debug_msg)



class User:
    def __init__(self, user_id, index=None, guid=None, is_active=None,
                 balance=None, picture=None, age=None, eye_color=None,
                 first_name=None, last_name=None, company=None, email=None,
                 phone=None, address=None, about=None, registered=None,
                 latitude=None, longitude=None, tags=None, user_range=None,
                 friends=None, greeting=None, favorite_fruit=None, digestive_value=None):
        self._id = user_id
        self._index = index
        self._guid = guid
        self._is_active = is_active
        self._balance = balance
        self._picture = picture
        self._age = age
        self._eye_color = eye_color
        self._first_name = first_name
        self._last_name = last_name
        self._company = company
        self._email = email
        self._phone = phone
        self._address = address
        self._about = about
        self._registered = registered
        self._latitude = latitude
        self._longitude = longitude
        self._tags = tags
        self._range = user_range
        self._friends = friends
        self._greeting = greeting
        self._favorite_fruit = favorite_fruit
        self._digestive_value = digestive_value

    def __str__(self):
        return f"{self._first_name} {self._last_name}"

    def GetField(self, field):
        if field not in allowed_fields:
            print(Colors.WARNING + f"InvalidFieldException: '{field}' is not allowed" + Colors.ENDC)
            print(Colors.WARNING + f"Allowed fields are {allowed_fields}" + Colors.ENDC)
            return
        else:
            if field == "id":
                return self._id
            if field == "index":
                return self._index
            if field == "guid":
                return self._guid
            if field == "is_active":
                return self._is_active
            if field == "balance":
                return self._balance
            if field == "picture":
                return self._picture
            if field == "age":
                return self._age
            if field == "eye_color":
                return self._eye_color
            if field == "first_name":
                return self._first_name
            if field == "last_name":
                return self._last_name
            if field == "company":
                return self._company
            if field == "email":
                return self._email
            if field == "phone":
                return self._phone
            if field == "address":
                return self._address
            if field == "about":
                return self._about
            if field == "registered":
                return self._registered
            if field == "latitude":
                return self._latitude
            if field == "longitude":
                return self._longitude
            if field == "tags":
                return self._tags
            if field == "range":
                return self._range
            if field == "friends":
                return self._friends
            if field == "greeting":
                return self._greeting
            if field == "favorite_fruit":
                return self._favorite_fruit
            if field == "digestive_value":
                return self._digestive_value
