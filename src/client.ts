import * as grpc from "@grpc/grpc-js"
import * as protoLoader from "@grpc/proto-loader"

const PROTO_PATH = "./pubsub.proto"

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
})

const proto = grpc.loadPackageDefinition(packageDefinition) as any

export default class Ceo {
  private client: any
  private subscriptions: Map<string, grpc.ClientDuplexStream<any, any>>

  constructor(host: string) {
    this.client = new proto.PubSubService(
      host,
      grpc.credentials.createInsecure()
    )
    this.subscriptions = new Map() // Para armazenar as inscrições ativas
  }

  pub(topic: string, callback: () => string) {
    const message = callback()
    this.client.publish({ topic, message }, (error: any) => {
      if (error) {
        console.error("Error publishing:", error)
      }
    })
  }

  sub(topic: string, onMessage: (message: string) => void) {
    const call = this.client.subscribe({ topic })
    this.subscriptions.set(topic, call)

    call.on("data", (response: any) => {
      onMessage(response.message) // Chama o callback quando uma nova mensagem for recebida
    })

    call.on("end", () => {
      console.log("End of stream")
    })
  }

  unsub(topic: string) {
    const call = this.subscriptions.get(topic)
    if (call) {
      call.cancel() // Cancela a assinatura
      this.subscriptions.delete(topic) // Remove a assinatura do mapa
      console.log(`Unsubscribed from topic: ${topic}`)
    } else {
      console.warn(`No subscription found for topic: ${topic}`)
    }
  }
}
