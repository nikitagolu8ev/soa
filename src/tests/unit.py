import unittest
import requests
import json
import time

class TestSocialNetworkMethods(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        requests.post("http://localhost:4200/users/register", data=json.dumps({
            "login": "login",
            "password": "password"
        }))
        requests.post("http://localhost:4200/users/register", data=json.dumps({
            "login": "second login",
            "password": "password"
        }))
        requests.post("http://localhost:4200/users/register", data=json.dumps({
            "login": "third login",
            "password": "password"
        }))
        requests.post("http://localhost:4200/users/register", data=json.dumps({
            "login": "forth login",
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
        return r.cookies, r.json()['UserId']

    def update_user(self, property, value, cookies):
        r = requests.put("http://localhost:4200/users/", data=json.dumps({
            property: value
        }), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

    def create_post(self, title, content, cookies):
        r = requests.post("http://localhost:4200/posts/", data=json.dumps({
            "Title": title,
            "Content": content,
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)
        return int(r.json()["PostId"])

    def update_post(self, post_id, title, content, cookies):
        r = requests.put("http://localhost:4200/posts/" + str(post_id), data=json.dumps({
            "Title": title,
            "Content": content,
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)

    def delete_post(self, post_id, cookies):
        r = requests.delete("http://localhost:4200/posts/" + str(post_id), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)

    def get_post(self, post_id, cookies):
        r = requests.get("http://localhost:4200/posts/" + str(post_id), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

        return r.json()["Post"]

    def get_page(self, page_id, cookies):
        r = requests.get("http://localhost:4200/posts/page/" + str(page_id), cookies=cookies.get_dict())
        self.assertEqual(r.status_code, 200)

        return r.json()["Posts"]
    
    def like_post(self, post_id, user_id, cookies):
        r = requests.put("http://localhost:4200/posts/like/" + str(post_id), data=json.dumps({
            "author_id": user_id,
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)

    def view_post(self, post_id, user_id, cookies):
        r = requests.put("http://localhost:4200/posts/view/" + str(post_id), data=json.dumps({
            "author_id": user_id,
        }), cookies=cookies.get_dict())

        self.assertEqual(r.status_code, 200)

    def get_stats(self, post_id):
        r = requests.get("http://localhost:4200/posts/stats/" + str(post_id))

        print(r.content)

        self.assertEqual(r.status_code, 200)

    def get_top_posts(self, order_by):
        r = requests.get("http://localhost:4200/posts/top/" + order_by)

        print(r.content)

        self.assertEqual(r.status_code, 200)

    def get_top_authors(self):
        r = requests.get("http://localhost:4200/users/top")

        print(r.content)

        self.assertEqual(r.status_code, 200)

    def test_login(self):
        self.login("login", "password")

    def test_update(self):
        cookies, _ = self.login("login", "password")
        self.update_user("email", "new_email@email.com", cookies)

    def test_post(self):
        cookies, _ = self.login("login", "password")

        post_id_1 = self.create_post("1st post", "some content", cookies)

        # print("First post:", self.get_post(post_id_1, cookies))

        self.update_post(post_id_1, "1st post (modified)", "some content (modified)", cookies)

        # print("Second post:", self.get_post(post_id_1, cookies))

        post_id_2 = self.create_post("2nd post", "new content", cookies)

        # print("Page with both posts:", self.get_page(0, cookies))

        self.delete_post(post_id_1, cookies)

        # print("Page after deleting first post:", self.get_page(0, cookies))

        self.delete_post(post_id_2, cookies)

        # r = requests.delete("http://localhost:4200/posts/" + str(post_id_1), cookies=cookies.get_dict())

        # print(r.status_code, r.content)


    def test_like(self):
        cookies, user_id = self.login("login", "password")

        post_id = self.create_post("Post to like", "Content to like", cookies)

        self.like_post(post_id, user_id, cookies)
        self.view_post(post_id, user_id, cookies)

        cookies, _ = self.login("second login", "password")

        self.like_post(post_id, user_id, cookies)
        self.like_post(post_id, user_id, cookies)


        time.sleep(1)

        self.get_stats(post_id)

        cookies, _ = self.login("login", "password")
        self.delete_post(post_id, cookies)
        self.get_stats(post_id)

    def test_top_posts(self):
        cookies, first_author = self.login("login", "password")

        first_post_id = self.create_post("Most liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)


        cookies, second_author = self.login("second login", "password")

        second_post_id = self.create_post("Second liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)
        self.like_post(second_post_id, second_author, cookies)

        cookies, third_author = self.login("third login", "password")

        third_post_id = self.create_post("Third liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)
        self.like_post(second_post_id, second_author, cookies)
        self.like_post(third_post_id, third_author, cookies)

        cookies, forth_author = self.login("forth login", "password")

        forth_post_id = self.create_post("Forth liked post", "...", cookies)
        fivth_post_id = self.create_post("Fivth liked post", "...", cookies)
        sixth_post_id = self.create_post("Sixth liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)
        self.like_post(second_post_id, second_author, cookies)
        self.like_post(third_post_id, third_author, cookies)
        self.like_post(forth_post_id, forth_author, cookies)
        self.like_post(sixth_post_id, forth_author, cookies)

        time.sleep(1)

        self.get_top_posts("likes")
        self.delete_post(forth_post_id, cookies)
        self.delete_post(fivth_post_id, cookies)
        self.delete_post(sixth_post_id, cookies)
        
        cookies, _ = self.login("third login", "password")
        self.delete_post(third_post_id, cookies)

        cookies, _ = self.login("second login", "password")
        self.delete_post(second_post_id, cookies)
    
        cookies, _ = self.login("login", "password")
        self.delete_post(first_post_id, cookies)

    def test_top_authors(self):
        cookies, first_author = self.login("login", "password")

        first_post_id = self.create_post("Most liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)


        cookies, second_author = self.login("second login", "password")

        second_post_id = self.create_post("Second liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)
        self.like_post(second_post_id, second_author, cookies)

        cookies, third_author = self.login("third login", "password")

        third_post_id = self.create_post("Third liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)
        self.like_post(second_post_id, second_author, cookies)
        self.like_post(third_post_id, third_author, cookies)

        cookies, forth_author = self.login("forth login", "password")

        forth_post_id = self.create_post("Forth liked post", "...", cookies)
        fivth_post_id = self.create_post("Fivth liked post", "...", cookies)
        sixth_post_id = self.create_post("Sixth liked post", "...", cookies)
        self.like_post(first_post_id, first_author, cookies)
        self.like_post(second_post_id, second_author, cookies)
        self.like_post(third_post_id, third_author, cookies)
        self.like_post(forth_post_id, forth_author, cookies)
        self.like_post(sixth_post_id, forth_author, cookies)

        time.sleep(1)

        self.get_top_authors()
        self.delete_post(forth_post_id, cookies)
        self.delete_post(fivth_post_id, cookies)
        self.delete_post(sixth_post_id, cookies)
        
        cookies, _ = self.login("third login", "password")
        self.delete_post(third_post_id, cookies)

        cookies, _ = self.login("second login", "password")
        self.delete_post(second_post_id, cookies)
    
        cookies, _ = self.login("login", "password")
        self.delete_post(first_post_id, cookies)



if __name__ == '__main__':
    unittest.main()
