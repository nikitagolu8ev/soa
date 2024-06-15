import unittest
import requests
import json

class TestSocialNetworkMethods(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        requests.post("http://localhost:4200/users/register", data=json.dumps({
            "login": "login",
            "password": "password"
        }))

    def login(self, login, password):
        r = requests.post("http://localhost:4200/users/login", data=json.dumps({
            "login": login,
            "password": password
        }))
        self.assertEqual(r.status_code, 200)
        cookie = r.cookies.get("token")
        self.assertIsNotNone(cookie)
        return r.cookies
    
    def update_user(self, property, value, cookies):
        r = requests.put("http://localhost:4200/users/update", data=json.dumps({
            property: value
        }), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

    def create_post(self, title, content, cookies):
        r = requests.post("http://localhost:4200/posts/create", data=json.dumps({
            "Title": title,
            "Content": content,
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)
        return int(r.json()["PostId"])
    
    def update_post(self, post_id, title, content, cookies):
        r = requests.put("http://localhost:4200/posts/update/" + str(post_id), data=json.dumps({
            "Title": title,
            "Content": content,
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)

    def delete_post(self, post_id, cookies):
        r = requests.delete("http://localhost:4200/posts/delete/" + str(post_id), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)

    def get_post(self, post_id, cookies):
        r = requests.get("http://localhost:4200/posts/get/" + str(post_id), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

        return r.json()["Post"]
    
    def get_page(self, page_id, cookies):
        r = requests.get("http://localhost:4200/posts/page/" + str(page_id), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

        return r.json()["Posts"]



    def test_login(self):
        self.login("login", "password")

    def test_update(self):
        cookies = self.login("login", "password")
        self.update_user("email", "new_email@email.com", cookies)

    def test_post(self):
        cookies = self.login("login", "password")

        post_id_1 = self.create_post("1st post", "some content", cookies)

        print("First post:", self.get_post(post_id_1, cookies))

        self.update_post(post_id_1, "1st post (modified)", "some content (modified)", cookies)

        print("Second post:", self.get_post(post_id_1, cookies))

        post_id_2 = self.create_post("2nd post", "new content", cookies)

        print("Page with both posts:", self.get_page(0, cookies))

        self.delete_post(post_id_1, cookies)

        print("Page after deleting first post:", self.get_page(0, cookies))

        self.delete_post(post_id_2, cookies)

if __name__ == '__main__':
    unittest.main()