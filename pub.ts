import { Ceo } from "./src"

const ceo = new Ceo("localhost:50051")

ceo.pub("event", () => {
    return "Sou foda"
})
