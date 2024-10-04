import { Ceo } from "./src/client"

const client = new Ceo("localhost:50051")

client.sub("event", (message) => {
  console.log(message)
})
