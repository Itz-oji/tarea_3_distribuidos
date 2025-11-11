package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	proto "example1/proto"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("10.35.168.62:55000", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Error al conectar con el broker: %v", err)
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	producerID := "Parisio-01"
	regResp, err := client.RegisterProducer(context.Background(), &proto.RegisterRequest{ProducerId: producerID, Store: "Parisio"})
	if err != nil || regResp == nil || !regResp.Ok {
		log.Fatalf("Registro de productor falló: %v, resp: %v", err, regResp)
	}

	f, err := os.Open("parisio_catalogo.csv")
	if err != nil {
		log.Fatalf("No se pudo abrir catálogo: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Error leyendo CSV: %v", err)
	}

	type Item struct {
		Category, Product string
		Price             float64
		Stock             int32
	}
	var items []Item
	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) < 6 {
			continue
		}
		precio, _ := strconv.ParseFloat(row[4], 64)
		stk64, _ := strconv.ParseInt(row[5], 10, 32)
		items = append(items, Item{row[2], row[3], precio, int32(stk64)})
	}
	if len(items) == 0 {
		log.Fatalf("Catálogo vacío para Parisio")
	}

	rand.Seed(time.Now().UnixNano())
	for {
		time.Sleep(time.Duration(1000+rand.Intn(1000)) * time.Millisecond)
		it := items[rand.Intn(len(items))]
		discount := 0.10 + rand.Float64()*(0.50-0.10)
		price := it.Price * (1.0 - discount)
		stock := it.Stock - int32(rand.Intn(3))
		if stock <= 0 {
			stock = 1
		}

		offer := &proto.Offer{
			OfertaId:   fmt.Sprintf("%d", time.Now().UnixNano()),
			ProductoId: producerID,
			Tienda:     "Parisio",
			Categoria:  it.Category,
			Producto:   it.Product,
			Precio:     price,
			Stock:      stock,
			Fecha:      time.Now().Unix(),
		}

		resp, err := client.SendOffer(context.Background(), offer)
		if err != nil {
			log.Printf("Error al enviar oferta: %v", err)
			continue
		}
		fmt.Printf("Oferta enviada: %s -> broker: %s\n", offer.OfertaId, resp.Mensaje)
	}
}


