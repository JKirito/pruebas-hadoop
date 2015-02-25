package funciones;

import org.apache.hadoop.io.IntWritable;

import entities.CeldaWritable;
import entities.CoordenadaWritable;

public class BuscadorCasilla {
	double anchoCelda;
	// double latitudMin;
	// double longitudMin;
	double latitud;
	double longitud;

	public BuscadorCasilla(CoordenadaWritable coordenada, double anchoCelda, double latitudMin, double longitudMin) {
		this.latitud = coordenada.getLatitud().get() - latitudMin;
		this.longitud = coordenada.getLongitud().get() - longitudMin;
//		System.out.println("Fila: " + this.latitud + " | ");
//		System.out.println("Columna: " + this.longitud + " | ");
		this.anchoCelda = anchoCelda;
		// this.latitudMin = latitudMin;
		// this.longitudMin = longitudMin;
	}

	public CeldaWritable celda() {
		CeldaWritable celda = new CeldaWritable();
		int columna = (int) (this.latitud / this.anchoCelda);
		int fila = (int) (this.longitud / this.anchoCelda);
		// System.out.println("Fila: " + this.latitud + " | " + fila);
		// System.out.println("Columna: " + this.longitud + " | " + columna);
		celda.setFila(new IntWritable(fila));
		celda.setColumna(new IntWritable(columna));
		return celda;
	}

}
