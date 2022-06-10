# import requests
from flask import Flask, render_template, request

app = Flask(__name__, template_folder='templates', static_folder='static')


@app.route("/", methods=["GET"])
def home():
    return render_template('home.html')


@app.route("/answer", methods=["GET", "POST"])
def answer():
    question = "Here will be the question"
    api_answer = "Here will be the answer"
    if request.method == 'POST':
        # x = requests.post(service_url, json={
        #     "question": request.form.get("question")
        # })
        question = "test"
        api_answer = "test"
    return render_template('answer.html', question=question, api_answer=api_answer)


def category_a_api():
    return ["Sorry!", "It seems there was some kind of problem during query."]

def category_b_api():
    # ["Sorry!", "There are no answers for the input You provided."]
    return ["Sorry!", "It seems there was some kind of problem during query."]

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
