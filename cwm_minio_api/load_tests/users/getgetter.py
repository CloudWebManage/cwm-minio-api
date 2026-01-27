from locust import task, FastHttpUser


class GetGetter(FastHttpUser):

    @task
    def get(self):
        res = self.client.get("/")
        res.raise_for_status()
