from cassandra.cluster import Cluster
from flask import Flask, render_template, request

# Web app
app = Flask(__name__, template_folder='templates', static_folder='static')
api_questions = {
    "category_a_1": "Return the aggregated statistics containing the number of created pages for each Wikipedia domain for each hour in the last 6 hours, excluding the last hour.",
    "category_a_2": "Return the statistics about the number of pages created by bots for each of the domains for the last 6 hours, excluding the last hour.",
    "category_a_3": "Return Top 20 users that created the most pages during the last 6 hours, excluding the last hour. ",
    "category_b_1": "Return the list of existing domains for which pages were created. ",
    "category_b_2": "Return all the pages which were created by the user with a specified user_id. ",
    "category_b_3": "Return the number of articles created for a specified domain. ",
    "category_b_4": "Return the page with the specified page_id. ",
    "category_b_5": "Return the id, name, and the number of created pages of all the users who created at least one page in a specified time range.",
}

# Connection to Cassandra to get answers to user's questions
cluster = Cluster(["cassandra"], port=9042)
session = cluster.connect("wiki_project")


@app.route("/", methods=["GET"])
def home():
    return render_template('home.html', questions=api_questions)


@app.route("/answer", methods=["GET", "POST"])
def answer(question="Here will be the question", api_answer="Here will be the answer"):
    if request.method == 'POST':
        question_number = request.form.get("return")

        if question_number.split("_")[1] == "a":
            question, api_answer = category_a_api(question_number)
        else:
            question, api_answer = category_b_api(question_number, request.form.get(question_number))

    return render_template('answer.html', question=question, api_answer=api_answer)


def category_a_api(number):
    try:
        return [api_questions[number], "Sorry!"]
    except:
        return [api_questions[number], "Sorry! It seems there was some kind of problem during query."]


def category_b_api(number, parameter):
    try:
        if number == "category_b_1":
            rows = session.execute("select * from existing_domains;")
            result = [["Domains"], [[row.domain] for row in rows]]
        elif number == "category_b_2":
            rows = session.execute(
                f"select page_id, page_title from pages_created_by_user_id where user_id='{parameter}';")
            result = [["Page ID", "Page Title"], [[row.page_id, row.page_title] for row in rows]]
        elif number == "category_b_3":
            rows = session.execute(f"select count(page_id) from domains_articles where domain='{parameter}';")
            result = [["Number of articles"], [[row.system_count_page_id] for row in rows]]
        elif number == "category_b_4":
            rows = session.execute(f"select page_title from pages_by_id where page_id='{parameter}';")
            result = [["Page Title"], [[row.page_title] for row in rows]]
        else:
            parameter = parameter.split(":")[0]
            rows = session.execute(
                f"select user_id, user_name, count(page_id) from user_pages_by_hour where hour={parameter} group by user_id;")
            result = [["User ID", "User Name", "Number of created pages"], []]
            for row in rows:
                number_of_articles = [i.system_count_page_id for i in session.execute(
                    f"select count(page_id) from pages_created_by_user_id where user_id='{row.user_id}';")]
                result[1].append([row.user_id, row.user_name, len(number_of_articles)])

        if not result[1]:
            result[1] = [["empty" for i in range(len(result[0]))]]
        return [api_questions[number], result]

    except Exception as ex:
        return [api_questions[number], [["Sorry!"], [["It seems there was some kind of problem during query."]]]]


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
