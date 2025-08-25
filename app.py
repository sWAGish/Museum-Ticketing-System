from flask import Flask, render_template, redirect, url_for, request, session, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_babel import Babel
from flask_cors import CORS
import stripe
from models import db
from chatbot import get_chatbot_response

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['STRIPE_PUBLIC_KEY'] = 'your_stripe_public_key'
app.config['STRIPE_SECRET_KEY'] = 'your_stripe_secret_key'
app.config['BABEL_DEFAULT_LOCALE'] = 'en'

db.init_app(app)
babel = Babel(app)
CORS(app)
stripe.api_key = app.config['STRIPE_SECRET_KEY']

def get_locale():
    return session.get('locale', 'en')

babel.init_app(app, locale_selector=get_locale)

@app.context_processor
def inject_get_locale():
    return dict(get_locale=get_locale)

@app.route('/set_locale/<locale>')
def set_locale(locale):
    session['locale'] = locale
    return redirect(request.referrer)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/services')
def services():
    return render_template('services.html')

@app.route('/view')
def view():
    return render_template('view.html')

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if request.method == 'POST':
        return "Thank you for your message!"
    return render_template('contact.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        from models import User
        username = request.form['username']
        password = request.form['password']
        user = User(username=username, password=password)
        db.session.add(user)
        db.session.commit()
        return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        from models import User
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username, password=password).first()
        if user:
            session['user_id'] = user.id
            return redirect(url_for('book_ticket'))
        else:
            return "Login failed"
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/book_ticket', methods=['GET', 'POST'])
def book_ticket():
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('login'))

    if request.method == 'POST':
        from models import Ticket
        age = int(request.form['age'])
        if age < 18:
            return "You must be 18 or older to book a ticket."
        ticket = Ticket(
            name=request.form['name'],
            age=age,
            email=request.form['email'],
            user_id=user_id
        )
        db.session.add(ticket)
        db.session.commit()
        return redirect(url_for('payment', ticket_id=ticket.id))
    return render_template('book_ticket.html')

@app.route('/my_tickets')
def my_tickets():
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('login'))

    from models import Ticket
    tickets = Ticket.query.filter_by(user_id=user_id).all()
    return render_template('my_tickets.html', tickets=tickets)

@app.route('/delete_ticket/<int:ticket_id>', methods=['POST'])
def delete_ticket(ticket_id):
    from models import Ticket
    Ticket.query.filter_by(id=ticket_id).delete()
    db.session.commit()
    return redirect(url_for('my_tickets'))

@app.route('/payment/<int:ticket_id>', methods=['GET', 'POST'])
def payment(ticket_id):
    if request.method == 'POST':
        try:
            stripe.PaymentIntent.create(
                amount=1000,
                currency='usd',
                payment_method=request.form['payment_method_id'],
                confirmation_method='manual',
                confirm=True
            )
            return "Payment successful!"
        except Exception as e:
            return f"An error occurred: {str(e)}", 400
    return render_template('payment.html', stripe_public_key=app.config['STRIPE_PUBLIC_KEY'], ticket_id=ticket_id)

@app.route('/chatbot', methods=['GET', 'POST'])
def chatbot():
    if request.method == 'POST':
        data = request.get_json()
        user_message = data.get('message')
        bot_response = get_chatbot_response(user_message)
        return jsonify({"response": bot_response})
    return render_template('chatbot.html')

@app.route('/test_locale')
def test_locale():
    return f"Current locale: {get_locale()}"

@app.teardown_appcontext
def shutdown_session(exception=None):
    db.session.remove()

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
