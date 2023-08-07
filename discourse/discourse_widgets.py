from .discourse_actor import DiscourseActor
import logging
from datetime import datetime
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class DiscourseWidgets:
    def __init__(self, actor: DiscourseActor, collection_refs):
        self.actor = actor
        self.collection_refs = collection_refs

    def is_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, dict) and not response:
            return False

        elif isinstance(response, list) and not response:
            return False

        return True

    def topics(self, **kwargs):
        page = 0
        total_topics = 0
        total_posts = 0
        total_replies = 0
        total_views = 0
        total_likes = 0

        data = True
        date_and_count = {}
        while data:
            data = self.actor.discourse_rest_make_request(f"/latest.json", variables={"page": page})

            topics = data["topic_list"]["topics"]
            if not self.is_valid(data) or not topics:
                logger.info(f"[!] Invalid or empty data returned")
                break

            for topic in topics:
                created_date = datetime.strptime(topic["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").date()
                created_date = created_date.strftime("%Y-%m-%d")  # format it to 'yyyy-mm-dd'
                if created_date in date_and_count:
                    date_and_count[created_date] += 1

                else:
                    date_and_count[created_date] = 1

                total_topics += 1
                total_posts += topic["posts_count"]
                total_replies += topic["reply_count"]
                total_views += topic["views"]
                total_likes += topic["like_count"]

            page += 1

        average_post_per_topic = total_posts / total_topics
        average_replies_per_topic = total_replies / total_topics
        average_views_per_topic = total_views / total_topics
        average_likes_per_topic = total_likes / total_topics

        if not date_and_count:
            logger.info(f"[#invalid] No topics for protocol ")
            return

        self.collection_refs["discourse"].document("topic_activity").set({"data": date_and_count})

        data = {
            "total_topics": total_topics,
            "total_posts": total_posts,
            "total_replies": total_replies,
            "total_views": total_views,
            "total_likes": total_likes,
            "average_post_per_topic": average_post_per_topic,
            "average_replies_per_topic": average_replies_per_topic,
            "average_views_per_topic": average_views_per_topic,
            "average_likes_per_topic": average_likes_per_topic,
        }

        for k, v in data.items():
            if isinstance(v, float):
                data[k] = round(v, 2)

        self.collection_refs["discourse"].document("topic_metrics").set({"data": data})

    def users(self, **kwargs):
        # Please note that the /directory_items endpoint may not return all users if some of them have very low activity levels. Depending on the Discourse setup, users might need to reach a certain level of activity before they are included in the directory.

        total_users = 0
        users_total_likes_received = 0
        users_total_likes_given = 0
        users_total_topics_entered = 0
        users_total_topic_count = 0
        users_total_post_count = 0
        users_total_posts_read = 0
        users_total_days_visited = 0

        users_average_likes_received = 0
        users_average_likes_given = 0
        users_average_topics_entered = 0
        users_average_topic_count = 0
        users_average_post_count = 0
        users_average_posts_read = 0
        users_average_days_visited = 0

        page = 0
        data = True
        while data:
            data = self.actor.discourse_rest_make_request(
                f"/directory_items",
                variables={
                    "order": "likes_received",
                    "desc": "true",
                    "period": "all",
                    "page": page,
                },
            )

            if not self.is_valid(data):
                logger.warning("[!] Invalid or empty data returned")
                continue

            users = data["directory_items"]
            if not users:
                logger.info(f"[#invalid] No users left in page {page} for protocol")
                break

            for user in users:
                total_users += 1
                users_total_likes_received += user["likes_received"]
                users_total_likes_given += user["likes_given"]
                users_total_topics_entered += user["topic_count"]
                users_total_topic_count += user["topic_count"]
                users_total_post_count += user["post_count"]
                users_total_posts_read += user["posts_read"]
                users_total_days_visited += user["days_visited"]
            page += 1

        users_average_likes_received = users_total_likes_received / total_users
        users_average_likes_given = users_total_likes_given / total_users
        users_average_topics_entered = users_total_topics_entered / total_users
        users_average_topic_count = users_total_topic_count / total_users
        users_average_post_count = users_total_post_count / total_users
        users_average_posts_read = users_total_posts_read / total_users
        users_average_days_visited = users_total_days_visited / total_users

        data = {
            "total_users": total_users,
            "users_total_likes_received": users_total_likes_received,
            "users_total_likes_given": users_total_likes_given,
            "users_total_topics_entered": users_total_topics_entered,
            "users_total_topic_count": users_total_topic_count,
            "users_total_post_count": users_total_post_count,
            "users_total_posts_read": users_total_posts_read,
            "users_total_days_visited": users_total_days_visited,
            "users_average_likes_received": users_average_likes_received,
            "users_average_likes_given": users_average_likes_given,
            "users_average_topics_entered": users_average_topics_entered,
            "users_average_topic_count": users_average_topic_count,
            "users_average_post_count": users_average_post_count,
            "users_average_posts_read": users_average_posts_read,
            "users_average_days_visited": users_average_days_visited,
        }

        for k, v in data.items():
            if isinstance(v, float):
                data[k] = round(v, 2)

        self.collection_refs["discourse"].document(f"user_metrics").set({"data": data})

    def categories(self, **kwargs):
        # formatting will be in frontend
        data = self.actor.discourse_rest_make_request(f"/categories.json")

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        categories = []
        for category in data["category_list"]["categories"]:
            category_data = {
                "id": category["id"],
                "name": category["name"],
                "slug": category["slug"],
                "color": category["color"],
                "topic_count": category["topic_count"],
                "post_count": category["post_count"],
                "num_featured_topics": category["num_featured_topics"],
                "topics_day": category["topics_day"],
                "topics_week": category["topics_week"],
                "topics_month": category["topics_month"],
                "topics_year": category["topics_year"],
                "topics_all_time": category["topics_all_time"],
                "description_text": category["description"],
                "subcategories": [],
            }

            for sub_c_id in category["subcategory_ids"]:
                sub_data = self.actor.discourse_rest_make_request(f"/c/{sub_c_id}/show.json")
                if not self.is_valid(sub_data):
                    logger.info(f'"[!] Invalid or empty data returned"')
                    continue
                sub_data = sub_data["category"]
                sub_category_data = {
                    "name": sub_data["name"],
                    "slug": sub_data["slug"],
                    "color": sub_data["color"],
                    "topic_count": sub_data["topic_count"],
                    "post_count": sub_data["post_count"],
                    "num_featured_topics": sub_data["num_featured_topics"],
                    "description_text": sub_data["description"],
                }
                category_data["subcategories"].append(sub_category_data)
            categories.append(category_data)

        self.collection_refs["discourse"].document("categories").set({"data": categories})

    def tags(self, **kwargs):
        # formatting will be in frontend
        data = self.actor.discourse_rest_make_request(f"/tags.json")

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        tags = []
        for tag in data["tags"]:
            tag_data = {
                "id": tag["id"],
                "text": tag["text"],
                "count": tag["count"],
            }
            tags.append(tag_data)

        sorted_tags = sorted(tags, key=lambda k: k["count"], reverse=True)
        self.collection_refs["discourse"].document("tags").set({"data": sorted_tags})

    def top_topics(self, **kwargs):
        topics = {}
        for interval in [
            "all",
            "yearly",
            "quarterly",
            "monthly",
            "weekly",
            "daily",
        ]:
            topics[interval] = []

            # formatting will be in frontend
            data = self.actor.discourse_rest_make_request(f"/top/{interval}.json")

            if not self.is_valid(data):
                logger.warning("[!] Invalid or empty data returned")
                continue

            for topic in data["topic_list"]["topics"]:
                topic_data = {
                    "id": topic["id"],
                    "title": topic["title"],
                    "slug": topic["slug"],
                    "posts_count": topic["posts_count"],
                    "reply_count": topic["reply_count"],
                    "highest_post_number": topic["highest_post_number"],
                    "like_count": topic["like_count"],
                    "views": topic["views"],
                    "created_at": topic["created_at"],
                    "last_posted_at": topic["last_posted_at"],
                    "posters_len": len(topic["posters"]),
                }
                topics[interval].append(topic_data)
                if len(topics[interval]) == 10:
                    break

        self.collection_refs["discourse"].document("top_topics").set({"data": topics})

    def latest_topics(self, **kwargs):
        latest_topics = {}
        for order in [
            "default",
            "created",
            "activity",
            "views",
            "posts",
            "likes",
            "op_likes",
        ]:
            latest_topics[order] = []

            # formatting will be in frontend
            data = self.actor.discourse_rest_make_request(f"/latest.json", variables={"order": order})

            if not self.is_valid(data):
                logger.warning("[!] Invalid or empty data returned")
                continue

            for topic in data["topic_list"]["topics"]:
                topic_data = {
                    "id": topic["id"],
                    "title": topic["title"],
                    "slug": topic["slug"],
                    "posts_count": topic["posts_count"],
                    "reply_count": topic["reply_count"],
                    "highest_post_number": topic["highest_post_number"],
                    "like_count": topic["like_count"],
                    "views": topic["views"],
                    "created_at": topic["created_at"],
                    "last_posted_at": topic["last_posted_at"],
                    "posters_len": len(topic["posters"]),
                }
                latest_topics[order].append(topic_data)
                if len(latest_topics[order]) == 10:
                    break

        self.collection_refs["discourse"].document("latest_topics").set({"data": latest_topics})

    def latest_posts(self, **kwargs):
        latest_posts = []
        # formatting will be in frontend
        data = self.actor.discourse_rest_make_request(f"/posts.json")

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for post in data["latest_posts"]:
            post_data = {
                "id": post["id"],
                "name": post["name"],
                "username": post["username"],
                "user_id": post["user_id"],
                "avatar_template": post["avatar_template"],
                "created_at": post["created_at"],
                "updated_at": post["updated_at"],
                "reply_count": post["reply_count"],
                "quote_count": post["quote_count"],
                "reads": post["reads"],
                "score": post["score"],
                "topic_id": post["topic_id"],
                "topic_slug": post["topic_slug"],
                "topic_title": post["topic_title"],
                "category_id": post["category_id"],
                "readers_count": post["readers_count"],
            }
            latest_posts.append(post_data)
            if len(latest_posts) == 10:
                break

        self.collection_refs["discourse"].document("latest_posts").set({"data": latest_posts})

    def top_users(self, **kwargs):
        top_users = {}
        for interval in [
            "all",
            "yearly",
            "quarterly",
            "monthly",
            "weekly",
            "daily",
        ]:
            top_users[interval] = {}
            for order in [
                "likes_received",
                "likes_given",
                "topic_count",
                "post_count",
                "topic_entered",
                "posts_read",
                "days_visited",
            ]:
                top_users[interval][order] = []

                # formatting will be in frontend
                data = self.actor.discourse_rest_make_request(
                    f"/directory_items",
                    variables={
                        "order": order,
                        "desc": "true",
                        "period": interval,
                    },
                )

                if not self.is_valid(data):
                    logger.warning("[!] Invalid or empty data returned")
                    continue

                for user in data["directory_items"]:
                    user_data = {
                        "id": user["id"],
                        "likes_received": user["likes_received"],
                        "likes_given": user["likes_given"],
                        "topics_entered": user["topics_entered"],
                        "topic_count": user["topic_count"],
                        "post_count": user["post_count"],
                        "posts_read": user["posts_read"],
                        "days_visited": user["days_visited"],
                        "user_id": user["user"]["id"],
                        "username": user["user"]["username"],
                        "name": user["user"]["name"],
                        "avatar_template": user["user"]["avatar_template"],
                    }
                    top_users[interval][order].append(user_data)
                    if len(top_users[interval][order]) == 10:
                        break

        self.collection_refs["discourse"].document(f"top_users").set({"data": top_users})
