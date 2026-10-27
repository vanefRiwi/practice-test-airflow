# Respuestas a las preguntas de negocio

Cada respuesta sale de una consulta en [`sql/queries/`](../sql/queries), ejecutable contra el repositorio analítico después de correr el DAG. Cifras en libras esterlinas (GBP), periodo del 01/12/2010 al 09/12/2011.

**Totales del periodo:** revenue bruto **10,25 M**, devoluciones **0,48 M**, revenue neto **9,77 M**.

---

## P1. Evolución mensual de las ventas netas

Consulta: [`q1_monthly_net_sales.sql`](../sql/queries/q1_monthly_net_sales.sql)

| Mes | Bruto | Devoluciones | Neto | % devolución |
|---|---:|---:|---:|---:|
| 2010-12 | 775.638 | 17.548 | 758.091 | 2,26 % |
| 2011-01 | 670.380 | 91.525 | 578.855 | **13,65 %** |
| 2011-02 | 507.800 | 8.336 | 499.464 | 1,64 % |
| 2011-03 | 689.834 | 10.504 | 679.329 | 1,52 % |
| 2011-04 | 515.378 | 33.316 | 482.062 | 6,46 % |
| 2011-05 | 739.938 | 8.948 | 730.990 | 1,21 % |
| 2011-06 | 737.559 | 13.714 | 723.845 | 1,86 % |
| 2011-07 | 688.219 | 11.407 | 676.812 | 1,66 % |
| 2011-08 | 724.217 | 22.927 | 701.290 | 3,17 % |
| 2011-09 | 1.028.320 | 16.981 | 1.011.340 | 1,65 % |
| 2011-10 | 1.103.314 | 41.613 | 1.061.701 | 3,77 % |
| 2011-11 | 1.451.579 | 24.991 | **1.426.588** | 1,72 % |
| 2011-12* | 614.491 | 174.091 | 440.400 | **28,33 %** |

\* Diciembre 2011 solo tiene datos hasta el día 9.

**Respuesta.** Las ventas netas se mueven entre 0,48 M y 0,73 M al mes hasta agosto y crecen fuerte desde septiembre: noviembre de 2011 es el pico con **1,43 M**, casi el doble de un mes típico del primer semestre. Septiembre a noviembre concentran el **35 %** del revenue bruto del periodo.

**Implicación.** El negocio es estacional (temporada navideña). Los picos de devolución de enero y diciembre no son estacionales: se explican por dos pedidos puntuales (ver P7).

---

## P2. Categorías por revenue bruto y proporción de devoluciones

Consulta: [`q2_category_revenue_returns.sql`](../sql/queries/q2_category_revenue_returns.sql)

| Categoría | Productos | Bruto | % del bruto | Devoluciones | % devolución (monto) |
|---|---:|---:|---:|---:|---:|
| Home | 1.904 | 6.126.783 | **59,79 %** | 239.573 | 3,91 % |
| Clothing | 535 | 1.204.941 | 11,76 % | 20.372 | 1,69 % |
| Stationery | 546 | 1.143.068 | 11,16 % | 184.719 | **16,16 %** |
| Uncategorized | 680 | 867.374 | 8,46 % | 13.422 | 1,55 % |
| Electronics | 135 | 474.246 | 4,63 % | 9.908 | 2,09 % |
| Sports | 127 | 430.256 | 4,20 % | 7.907 | 1,84 % |

**Respuesta.** **Home** genera casi 6 de cada 10 libras de venta bruta. **Stationery** tiene la mayor proporción de devoluciones (16,16 %), seguida de Home (3,91 %).

**Implicación.** El 16,16 % de Stationery no es un problema de la categoría: si se excluye el producto 23843 (un solo pedido cancelado, ver P7) su tasa baja a **1,67 %**, en línea con el resto.

---

## P3. Top 10 por revenue neto y top 10 por tasa de devolución

Consulta: [`q3_top_products.sql`](../sql/queries/q3_top_products.sql)

**Top 10 por revenue neto**

| # | Código | Producto | Categoría | Neto |
|---:|---|---|---|---:|
| 1 | 22423 | REGENCY CAKESTAND 3 TIER | Home | 164.459 |
| 2 | 85123A | WHITE HANGING HEART T-LIGHT HOLDER | Home | 99.791 |
| 3 | 47566 | PARTY BUNTING | Home | 98.244 |
| 4 | 85099B | JUMBO BAG RED RETROSPOT | Clothing | 92.176 |
| 5 | 23084 | RABBIT NIGHT LIGHT | Electronics | 66.662 |
| 6 | 22086 | PAPER CHAIN KIT 50'S CHRISTMAS | Stationery | 63.715 |
| 7 | 84879 | ASSORTED COLOUR BIRD ORNAMENT | Home | 58.792 |
| 8 | 79321 | CHILLI LIGHTS | Electronics | 53.747 |
| 9 | 22502 | PICNIC BASKET WICKER SMALL | Sports | 51.024 |
| 10 | 22197 | POPCORN HOLDER | Home | 50.968 |

**Top 10 por tasa de devolución** (solo productos con al menos 1.000 GBP de venta bruta)

| # | Código | Producto | Bruto | Devuelto | % devolución |
|---:|---|---|---:|---:|---:|
| 1 | 23843 | PAPER CRAFT , LITTLE BIRDIE | 168.470 | 168.470 | 100,00 % |
| 2 | 23166 | MEDIUM CERAMIC TOP STORAGE JAR | 81.701 | 77.480 | 94,83 % |
| 3 | 85232B | SET OF 3 BABUSHKA STACKING TINS | 1.356 | 1.258 | 92,76 % |
| 4 | 23113 | PANTRY CHOPPING BOARD | 5.869 | 4.803 | 81,84 % |
| 5 | 23056 | FLOWERS CHANDELIER T-LIGHT HOLDER | 2.898 | 1.859 | 64,15 % |
| 6 | 23055 | IVORY CHANDELIER T-LIGHT HOLDER | 1.395 | 691 | 49,51 % |
| 7 | 22313 | OFFICE MUG WARMER PINK | 1.565 | 638 | 40,74 % |
| 8 | 23064 | CINDERELLA CHANDELIER | 3.718 | 1.514 | 40,73 % |
| 9 | 22920 | HERB MARKER BASIL | 2.153 | 841 | 39,06 % |
| 10 | 21108 | FAIRY CAKE FLANNEL ASSORTED COLOUR | 18.056 | 6.591 | 36,50 % |

**Implicación.** El mínimo de 1.000 GBP evita que un producto con una sola venta devuelta aparezca con 100 %. Las tres lámparas de araña "CHANDELIER" aparecen juntas: es una línea de producto con problemas, no un caso aislado.

---

## P4. Países por transacciones y ticket promedio

Consulta: [`q4_country_avg_ticket.sql`](../sql/queries/q4_country_avg_ticket.sql)

| País | Facturas | % facturas | Bruto | Ticket promedio |
|---|---:|---:|---:|---:|
| United Kingdom | 17.901 | **90,53 %** | 8.723.892 | 487,34 |
| Germany | 443 | 2,24 % | 205.381 | 463,61 |
| France | 382 | 1,93 % | 184.493 | 482,97 |
| Ireland | 282 | 1,43 % | 270.851 | 960,46 |
| Belgium | 98 | 0,50 % | 36.927 | 376,81 |
| Netherlands | 93 | 0,47 % | 283.889 | **3.052,57** |
| Spain | 88 | 0,45 % | 55.707 | 633,03 |
| Australia | 56 | 0,28 % | 138.104 | 2.466,14 |

**Respuesta.** El Reino Unido concentra el **90,5 %** de las facturas. Fuera de él, Alemania, Francia e Irlanda suman otro 5,6 %. El ticket varía mucho: Reino Unido, Alemania y Francia rondan **460-490 GBP**, mientras que Países Bajos (**3.053**) y Australia (**2.466**) tienen tickets 5 a 6 veces mayores con pocos clientes (9 cada uno).

**Implicación.** Los mercados internacionales grandes en ticket son de mayoristas: pocos clientes, compras grandes. Perder uno de esos clientes pesa mucho en el país.

---

## P5. Clientes identificados vs. transacciones sin CustomerID

Consulta: [`q5_identified_vs_anonymous.sql`](../sql/queries/q5_identified_vs_anonymous.sql)

| Tipo | Facturas | % del bruto | Ticket promedio | Productos por factura | Unidades por línea | Precio unitario prom. | % devolución |
|---|---:|---:|---:|---:|---:|---:|---:|
| Identificado | 18.402 | 85,26 % | 474,77 | 21,3 | 13,1 | 2,87 | 5,40 % |
| Sin CustomerID | 1.371 | 14,74 % | **1.101,38** | **95,8** | 3,2 | 4,46 | 0,27 % |

**Respuesta.** Sí hay una diferencia clara. Las facturas sin cliente son pocas (1.371) pero muy largas: casi **96 productos distintos por factura**, con pocas unidades de cada uno y precio unitario más alto. Casi no tienen devoluciones (0,27 %). El patrón parece de ventas al detal (tienda física o pedidos sin registro), no de mayoristas.

**Implicación.** Excluir estas transacciones habría borrado el **14,74 %** del revenue. Conviene que DataMart capture el cliente también en ese canal.

---

## P6. Productos sin descripción consistente y total de códigos

Consulta: [`q6_inconsistent_descriptions.sql`](../sql/queries/q6_inconsistent_descriptions.sql)

| Métrica | Valor |
|---|---:|
| Códigos de producto únicos en el DWH | **3.927** |
| Con más de una descripción | **637 (16,22 %)** |
| Sin ninguna descripción en las fuentes | 97 |
| Códigos que no son producto (rechazados) | 16 distintos (POST, DOT, M, C2, D, S...) |
| Códigos distintos en el archivo crudo | 3.958 |

Los casos más extremos: `20713` JUMBO BAG OWLS (8 variantes), `23084` RABBIT NIGHT LIGHT (7), `21830` ASSORTED CREEPY CRAWLIES y `85175` CACTI T-LIGHT CANDLES (6). Las variantes suelen ser notas de ajuste escritas a mano (`damaged`, `check`, `?`) o cambios de nombre del catálogo.

**Implicación.** Uno de cada seis productos tiene la descripción inconsistente: el sistema operacional permite texto libre en un campo que debería venir del catálogo.

---

## P7. Recomendación al equipo de producto

Consulta: [`q7_product_recommendation.sql`](../sql/queries/q7_product_recommendation.sql)

**Hallazgo.** Dos pedidos explican más de la mitad de todas las devoluciones del periodo:

| Código | Producto | Unidades vendidas y devueltas el mismo día | Monto | % de todas las devoluciones |
|---|---|---|---:|---:|
| 23843 | PAPER CRAFT , LITTLE BIRDIE | 80.995 (09/12/2011) | 168.470 | 41,14 % |
| 23166 | MEDIUM CERAMIC TOP STORAGE JAR | 74.215 (18/01/2011) | 77.480 | 18,92 % |

Juntos suman **245.949 GBP (51,7 % de las devoluciones)** y son la causa de los picos de enero (13,65 %) y diciembre (28,33 %) de la P1.

**Recomendación.** Poner un **control de cantidad máxima por línea de pedido** (por ejemplo, confirmación obligatoria por encima de 10.000 unidades, cuando la línea de venta típica es de 13 unidades). Con ese control, la tasa de devolución del periodo habría pasado de **4,64 % a 2,30 %**, y el revenue bruto deja de inflarse con pedidos que se cancelan el mismo día.

**Segunda prioridad.** Revisar la línea **"fairy cake"** de Home: `21108` FAIRY CAKE FLANNEL (36,50 % de devolución sobre 18.056 GBP) y `48185` DOORMAT FAIRY CAKE (19,08 % sobre 23.873 GBP) devuelven juntos 11.146 GBP. Igual con las lámparas **"CHANDELIER"** (`23056`, `23055`, `23064`), que superan el 40 % de devolución. Antes de la temporada septiembre-noviembre, que concentra el 35 % de las ventas, conviene revisar la calidad o la foto y descripción de estos productos.
