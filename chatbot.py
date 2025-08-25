import re

# List of dynamic patterns
patterns = {
    r'(hello|hi|hey)': "Hello! How can I assist you today?",
    r'(help|support)': "I'm here to help! You can ask me about services, ticket booking, and more.",
    r'(book.*ticket)': "Sure! You can book your ticket via our online booking system.",
    r'(bye|goodbye)': "Goodbye! Have a great day.",
    r'(services|service)': "We offer: Ticket Booking | Ticekt Cancellation | Museum View",
    r'(pricing|price)': "We costs Rs 100 per ticekt",
    r'(timing|time)': "We provide services 24x7",
    r'(i want to buy.*ticket)': "Of course! How many tickets would you like to book?",
    r'(for.*people|[0-9]+ ticket[s]?)': "Got it. Please mention the show name you'd like to attend.",
    r'(show|show name is|i want to see|for show)': "Perfect. Please confirm the date for your visit (e.g., 2025-03-01).",
    r'(date is|on .* \d{4})': "Thank you. Would you like to proceed with payment now?",
    r'(yes|proceed|go ahead|sure)': "Redirecting you to our secure payment gateway... 💳",
    r'(done|payment done|paid)': "Payment received! 🎉 Your tickets have been booked successfully.",
    r'(ticket status|check booking|my ticket)': "Let me check... ✅ Your ticket is confirmed. Enjoy your visit!",
    r'(cancel.*ticket)': "I’m sorry to hear that. Please provide your booking ID for cancellation.",
    r'(my booking id is .*|\bID\b \d{4,})': "Thank you. Your cancellation request is being processed.",
    r'(refund|money back)': "Refunds will be processed within 3-5 business days to your original payment method.",
    r'(policies|guidelines)': '''1. Person below age of 18 not allowed to book ticket.
                                 2. Ticekts can be cancelled within 48hrs of booking.
                                 3. Same person cannot book multiple tickets.
                                 4. Refund will be not allowed if you are unable to visit museum.'''
}

def get_chatbot_response(user_message):
    user_message = user_message.lower()

    for pattern, response in patterns.items():
        if re.search(pattern, user_message):
            return response

    return "I'm sorry, I don't understand that. Could you please clarify?"

