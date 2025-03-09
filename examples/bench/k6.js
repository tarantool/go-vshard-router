import grpc from 'k6/net/grpc'
import { sleep, check } from 'k6'

const client = new grpc.Client()
client.load(['webserver'], 'echo.proto')

export default function () {
    client.connect('0.0.0.0:8081', { plaintext: true })

    const data = { message: 'Hello'}
    console.log("invoke request")
    const response = client.invoke('echo.EchoService/Echo', data)
    console.log("invoke done")
    check(response, {
        'status is OK': (r) => r && r.status === grpc.StatusOK,
    })

    console.log(JSON.stringify(response.message))

    client.close()
    sleep(1)
}
