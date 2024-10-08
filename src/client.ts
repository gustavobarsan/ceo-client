import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from 'path';

const PROTO_PATH = path.resolve(__dirname, '../protos/pubsub.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

export class Ceo {
  private client: any;
  private subscriptions: Map<string, grpc.ClientDuplexStream<any, any>>;

  constructor(host: string) {
    this.client = new proto.pubsub.PubSubService(
      host,
      grpc.credentials.createInsecure()
    );
    this.subscriptions = new Map();
  }

  pub(topic: string, callback: () => string) {
    const message = callback();
    if (!message) {
      console.error("Message cannot be empty");
      return;
    }

    this.client.publish({ topic, message }, (error: any) => {
      if (error) {
        console.error("Error publishing:", error);
      }
    });
  }

  sub(topic: string, onMessage: (message: string) => void) {
    console.log(`Subscribing to topic: ${topic}`);
    const call = this.client.subscribe({ topic });
    this.subscriptions.set(topic, call);

    call.on("data", (response: any) => {
      onMessage(response.message);
    });

    call.on("end", () => {
      console.log(`End of stream for topic: ${topic}`);
    });

    call.on("error", (error: any) => {
      console.error(`Error in subscription for topic ${topic}:`, error);
    });
  }

  unsub(topic: string) {
    const call = this.subscriptions.get(topic);
    if (call) {
      call.cancel();
      this.subscriptions.delete(topic);
      console.log(`Unsubscribed from topic: ${topic}`);
    } else {
      console.warn(`No subscription found for topic: ${topic}`);
    }
  }
}