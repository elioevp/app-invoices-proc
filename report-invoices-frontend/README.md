# Cliente para App de Reportes

Interfaz de usuario para generar y visualizar reportes.

## Cómo Empezar

Sigue estas instrucciones para tener una copia del proyecto corriendo en tu máquina local para desarrollo y pruebas.

### Prerrequisitos

Necesitas tener Node.js y npm instalados en tu sistema.

- [Node.js](https://nodejs.org/)

### Instalación

1. Clona el repositorio (si está en GitHub) o descarga los archivos.
2. Abre una terminal en la raíz del proyecto.
3. Instala las dependencias del proyecto con npm:

   ```sh
   npm install
   ```

## Uso

### Iniciar el servidor de desarrollo

Para correr la aplicación en modo de desarrollo con recarga en caliente:

```sh
npm run dev
```

Esto iniciará la aplicación en `http://localhost:5173` (o el puerto que Vite asigne).

### Compilar para producción

Para crear una versión optimizada de la aplicación para producción:

```sh
npm run build
```

Los archivos compilados se guardarán en el directorio `dist`.

### Linting

Para revisar el código en busca de errores de estilo y potenciales problemas:

```sh
npm run lint
```

### Vista previa de la compilación

Para ejecutar un servidor local con la versión de producción (después de haber corrido `npm run build`):

```sh
npm run preview

##end
```