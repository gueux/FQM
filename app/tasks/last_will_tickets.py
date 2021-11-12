from app.tasks.base import TaskBase
from app.database import Serial
from app.middleware import db, mqtt

from app.constants import MQTT_LAST_WILL_TOPIC

class LastWillTickets(TaskBase):
    def __init__(self, app, interval=0):
        '''Task to remove tickets for aborter (non-programmatically disconnected) games.

        Parameters
        ----------
        app : Flask app
        interval : int, optional
            duration of sleep between iterations in seconds, 0 = infinity loop
        '''
        super().__init__(app)
        self.app = app
        self.interval = interval

        try:
            print("Try")
            mqtt.init_app(app)
            print("Init")
        except Exception as e:
            print(e)

    def run(self):
        @self.execution_loop()
        def main():
            print("Main")
            self.log(f'LastWillTickets(Task): started.')
            mqtt._connect_handler = self.on_connect
            mqtt.client.on_message = self.on_message
            # printr(mqtt.on_connect)

    def on_connect(self, client, userdata, flags, rc):
        mqtt.subscribe(MQTT_LAST_WILL_TOPIC)
        print("Subscribed for Queue Last Will messages")

    def on_message(self, client, userdata, message):
        state = message.payload.decode()
        if state == 'lost':
            _, ticket_id, _ = message.topic.split('/')
            print("Lost connection with user ticket: ", ticket_id)
            with self.app.app_context():
                try:
                    print("Trying to remove lost ticket")
                    ticket = Serial.query.get(ticket_id)
                    db.session.delete(ticket)
                    db.session.commit()
                    print("Ticket deleted: ", ticket_id)
                except Exception as e:
                    print(e)