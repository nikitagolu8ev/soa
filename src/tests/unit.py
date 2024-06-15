from pprint import pprint
import time
import unittest
import requests
from enum import Enum
import json

class Handles(Enum):
    REGISTER = 1
    LOGIN = 2
    UPDATE_USER = 3
    POST_CREATE = 4
    POST_UPDATE = 5
    POST_DELETE = 6
    POST_GET = 7
    PAGE_GET = 8
    POST_VIEW = 9
    POST_LIKE = 10

class TestSocialNetworkMethods(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("setting up")
        self.host = "http://localhost:4200/"
        self.addrs = {
            Handles.REGISTER: self.host + "users/register",
            Handles.LOGIN: self.host + "users/login",
            Handles.UPDATE_USER: self.host + "users/update",
            Handles.POST_CREATE: self.host + "posts/create",
            Handles.POST_UPDATE: self.host + "posts/update",
            Handles.POST_DELETE: self.host + "posts/delete",
            Handles.POST_GET: self.host + "posts/get",
            Handles.PAGE_GET: self.host + "posts/page",
            Handles.POST_VIEW: self.host + "posts/view/",
            Handles.POST_LIKE: self.host + "posts/like/",

        }
        self.login = "login"
        self.password = "password"

        healthy = False
        for _ in range(10):
            r = requests.get("http://localhost:8192/stat/ping")
            if r.status_code == 200:
                healthy = True
                print("Healthy stat service")
                break
            else:
                print(r.content)
                time.sleep(1)

        if not healthy:
            print("Unhealthy stats service! Exiting...")
            exit(1)

        # register once
        r = requests.post(self.addrs[Handles.REGISTER], data=json.dumps({
            "login": self.login,
            "password": self.password
        }))


    def try_login(self):
        r = requests.post(self.addrs[Handles.LOGIN], data=json.dumps({
            "login": self.login,
            "password": self.password
        }))
        self.assertEqual(r.status_code, 200)
        jwt_cookie = r.cookies.get("token")
        self.assertIsNotNone(jwt_cookie)
        return r.cookies


    def test_login(self):
        self.try_login()


    def test_update(self):
        cookies = self.try_login()

        # update
        r = requests.put(self.addrs[Handles.UPDATE_USER], data=json.dumps({
            "email": "qwe@mail.ru"
        }), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)


    def test_post(self):
        cookies = self.try_login()

        r = requests.post(self.addrs[Handles.POST_CREATE], data=json.dumps({
            "Title": "post#1",
            "Content": "some contents",
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)
        post_id_1 = int(r.json()["PostId"])

        r = requests.get(self.addrs[Handles.POST_GET] + f"/{post_id_1}", cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)
        self.assertIn("post#1", r.text)

        r = requests.put(self.addrs[Handles.POST_UPDATE] + f"/{post_id_1}", data=json.dumps({
            "Title": "post#0 (updated)",
            "Content": "brand new data",
        }), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

        r = requests.get(self.addrs[Handles.POST_GET] + f"/{post_id_1}", cookies=cookies.get_dict())
        self.assertIn("post#0 (updated)", r.text)

        r = requests.post(self.addrs[Handles.POST_CREATE], data=json.dumps({
            "Title": "post#2",
            "Content": "abacaba",
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)
        post_id_2 = int(r.json()["PostId"])

        r = requests.get(self.addrs[Handles.PAGE_GET] + "/0", cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)
        self.assertIn("post#0 (updated)", r.text)
        self.assertIn("post#2", r.text)

        r = requests.delete(self.addrs[Handles.POST_DELETE] + f"/{post_id_1}", cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

        r = requests.delete(self.addrs[Handles.POST_DELETE] + f"/{post_id_2}", cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

    def view_post(self, postId: int):
        cookies = self.try_login()
        print(self.addrs[Handles.POST_VIEW] + str(postId))
        r = requests.put(self.addrs[Handles.POST_VIEW] + str(postId), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)
        time.sleep(1)

    def like_post(self, postId: int):
        cookies = self.try_login()
        r = requests.put(self.addrs[Handles.POST_LIKE] + str(postId), cookies=cookies.get_dict())
        print('like: ', r.content)
        self.assertEqual(r.status_code, 200)
        time.sleep(1)

    def get_post_stats(self, postId: int):
        r = requests.get(f"http://localhost:8192/stat/likes/{postId}")

        print(r.content)
        self.assertEqual(r.status_code, 200)

    def test_stats(self):
        cookies = self.try_login()

        data = {
            "Title": "post_to_like",
            "Content": "very likable",
        }
        r = requests.post(self.addrs[Handles.POST_CREATE], data=json.dumps(data), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)
        postId = int(r.json()["PostId"])

        # self.view_post(postId)

        self.get_post_stats(postId)
        # self.assertEqual(statsPostId, postId)
        # self.assertEqual(views, 1)
        # self.assertEqual(likes, 0)

        # self.view_post(postId)

        self.get_post_stats(postId)
        # self.assertEqual(statsPostId, postId)
        # self.assertEqual(views, 2)
        # self.assertEqual(likes, 0)

        print(postId)
        self.like_post(postId)
        self.get_post_stats(postId)
        # self.assertEqual(statsPostId, postId)
        # self.assertEqual(views, 2)
        # self.assertEqual(likes, 1)

        r = requests.delete(self.addrs[Handles.POST_DELETE] + f"/{postId}", cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)





if __name__ == '__main__':
    unittest.main()