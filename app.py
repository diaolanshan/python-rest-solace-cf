from flask import Flask, request
import os
import json

import requests
from requests.auth import HTTPBasicAuth
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
app.config.from_object(__name__)
api = Api(app)


port = int(os.getenv('VCAP_APP_PORT', 80))

queue_name = 'python-rest-solace-queue'

vcap_services = json.loads(os.environ['VCAP_SERVICES'])
services = vcap_services.keys()
for service in services:
    if 'solace-pubsub' in vcap_services[service][0]['tags']:
        # Use the first 'rabbitmq' service.
        management_host = 'http://' + vcap_services[service][0]['credentials']['activeManagementHostname']
        management_username = vcap_services[service][0]['credentials']['managementUsername']
        management_password = vcap_services[service][0]['credentials']['managementPassword']
        msg_vpn = vcap_services[service][0]['credentials']['msgVpnName']
        restUris = vcap_services[service][0]['credentials']['restUris'][0]
        client_username = vcap_services[service][0]['credentials']['clientUsername']
        client_password = vcap_services[service][0]['credentials']['clientPassword']
        break

auth = HTTPBasicAuth(management_username, management_password)


@app.route('/rest/solace/queue', methods=['GET', 'POST'])
def define_queue():
    '''
    Use the SEMP v2 to create a queue which is call {python-rest-solace-queue}
    :return: a string which indicate whether the queue created successfully or not.
    '''
    request_body = {
        "queueName": queue_name,
        "accessType": "non-exclusive",
        "permission": "consume",
        "ingressEnabled": True,
        "egressEnabled": True
    }
    response = requests.post(url=management_host + '/SEMP/v2/config/msgVpns/{}/queues'.format(msg_vpn),
                             json=request_body,
                             auth=auth)

    return 'Queue created successfully' if response.status_code == 200 else 'Error while create queue, reason: '.format(
        response.reason)


class Solace(Resource):
    last_message = ''
    def put(self):
        '''
        Post a message to the solace pubsub+

        :return: return a success message if got 200 response from the vmr, otherwise, return a error message associated
        with a detailed reason of that.
        '''
        url = restUris + '/queue/' + queue_name
        response = requests.post(url=url,
                                 auth=HTTPBasicAuth(client_username, client_password), json={'message':'I a just a test message'})
        return 'Message published successfully' if response.status_code == 200 else 'Error while publish message, reason: '.format(
            response.reason)

    def post(self):
        '''
        Act as the solace consumer, listen to the push request from solace.
        :return:
        '''
        data = request.get_json()
        print('Got message:'.format(data))
        #
        # request.form['message']
        # print('Got message:'.format(request.form['message']))

        return "OK", 200

    def get(self):
        '''
        :return: the last message which the solace pushed to the current app.
        '''
        return self.last_message


api.add_resource(Solace, '/rest/solace/message')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False)
