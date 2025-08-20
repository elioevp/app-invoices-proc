import { useState, useEffect } from 'react';
import axios from 'axios';
import jsPDF from 'jspdf';
import 'jspdf-autotable';

const DashboardPage = () => {
    const [username, setUsername] = useState('');
    const [directorio, setDirectorio] = useState('');
    const [anticipo, setAnticipo] = useState('');
    const [reportData, setReportData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        const searchParams = new URLSearchParams(window.location.search);
        const userFromUrl = searchParams.get('username');
        if (userFromUrl) {
            setUsername(userFromUrl);
        }
    }, []);

    const handleFetchReport = async (e) => {
        e.preventDefault();
        if (!username || !directorio) {
            setError('Por favor, ingrese un usuario y un directorio.');
            return;
        }
        
        setLoading(true);
        setError('');
        setReportData(null);

        try {
                                               const apiUrl = import.meta.env.VITE_API_URL || 'https://report1-invoices-backend-aqe2gzbrbqenhtey.eastus2-01.azurewebsites.net/api' || 'http://localhost:3000/api';
            const response = await axios.get(`${apiUrl}/report`, {
                params: {
                    username: username,
                    directorio: directorio
                }
            });
            setReportData(response.data);
        } catch (err) {
            console.error('Error fetching report:', err);
            const errorMessage = err.response?.data || 'Ocurrió un error al buscar el reporte.';
            setError(typeof errorMessage === 'string' ? errorMessage : JSON.stringify(errorMessage));
        } finally {
            setLoading(false);
        }
    };

    const generatePdf = () => {
        if (!reportData) return;

        const doc = new jsPDF();
        const anticipoNum = Number(anticipo || 0);
        const montoTotal = reportData.monto_total_calculado;
        const diferencia = anticipoNum - montoTotal;

        // Title
        doc.text('Reporte de Liquidación', 14, 20);

        // Summary Info
        doc.setFontSize(12);
        doc.text(`Usuario: ${reportData.username}`, 14, 30);
        doc.text(`Directorio: ${reportData.directorio}`, 14, 36);
        doc.text(`Anticipo: Bs. ${anticipoNum.toFixed(2)}`, 14, 42);
        doc.text(`Monto Total facturas: Bs. ${montoTotal.toFixed(2)}`, 14, 48);

        if (diferencia > 0) {
            doc.text(`Diferencia a Pagar: Bs. ${diferencia.toFixed(2)}`, 14, 54);
        } else if (diferencia < 0) {
            doc.text(`Diferencia a Cobrar: Bs. ${(-diferencia).toFixed(2)}`, 14, 54);
        } else {
            doc.text(`Diferencia: Bs. 0.00`, 14, 54);
        }

        // Table
        const tableColumn = ["ID Factura", "Monto (Bs.)", "Fecha"];
        const tableRows = [];

        reportData.facturas.forEach(factura => {
            const facturaData = [
                factura.id,
                factura.montoTotal.toFixed(2),
                factura.fechaTransaccion
            ];
            tableRows.push(facturaData);
        });

        doc.autoTable({
            head: [tableColumn],
            body: tableRows,
            startY: 65,
        });

        // Save
        doc.save(`reporte-${reportData.directorio}.pdf`);
    };

    return (
        <div className="container py-5">
            {/* Encabezado Atractivo */}
            <div className="text-center mb-5">
                <h1 className="display-5 fw-bold">Reporte de Liquidación</h1>
                <p className="lead text-danger fw-bold">Consulte la relación final de facturas por usuario y directorio.</p>
            </div>

            {/* Tarjeta de Consulta */}
            <div className="card shadow-sm border-light mb-4">
                <div className="card-body p-4">
                    <h5 className="card-title mb-3">Consultar Reporte</h5>
                    <form onSubmit={handleFetchReport}>
                        <div className="row align-items-end">
                            <div className="col-lg-4 col-md-6 mb-3">
                                <label className="form-label fw-bold text-dark">Usuario</label>
                                <input
                                    type="text"
                                    className="form-control"
                                    value={username}
                                    onChange={(e) => setUsername(e.target.value)}
                                    placeholder="ej: elio villalobos"
                                    required
                                    readOnly
                                />
                            </div>
                            <div className="col-lg-3 col-md-6 mb-3">
                                <label className="form-label fw-bold text-dark">Directorio</label>
                                <input
                                    type="text"
                                    className="form-control"
                                    value={directorio}
                                    onChange={(e) => setDirectorio(e.target.value)}
                                    placeholder="ej: liquidacion-abril25"
                                    required
                                />
                            </div>
                            <div className="col-lg-3 col-md-6 mb-3">
                                <label className="form-label fw-bold text-dark">Anticipo (Bs.)</label>
                                <input
                                    type="number"
                                    className="form-control"
                                    step="0.01"
                                    value={anticipo}
                                    onChange={(e) => setAnticipo(e.target.value)}
                                    placeholder="0.00"
                                />
                            </div>
                            <div className="col-lg-2 col-md-6 mb-3">
                                <button type="submit" className="btn btn-primary w-100" disabled={loading}>
                                    {loading ? (
                                        <>
                                            <span className="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
                                            <span className="visually-hidden">Buscando...</span>
                                        </>
                                    ) : 'Consultar'}
                                </button>
                            </div>
                        </div>
                    </form>
                </div>
            </div>

            {error && <div className="alert alert-danger shadow-sm">{error}</div>}

            {/* Tarjeta de Resultados */}
            {reportData && (
                <div className="card shadow-sm border-light">
                    <div className="card-header d-flex justify-content-between align-items-center py-3">
                        <h5 className="mb-0">Resultados del Reporte</h5>
                        <button className="btn btn-success btn-sm" onClick={generatePdf}>
                            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" className="bi bi-download me-2" viewBox="0 0 16 16">
                                <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z"/>
                                <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z"/>
                            </svg>
                            Descargar PDF
                        </button>
                    </div>
                    <div className="card-body p-4">
                        <div className="row mb-4">
                            <div className="col-md-4">
                                <p className="mb-1"><strong>Usuario:</strong> {reportData.username}</p>
                                <p className="mb-1"><strong>Directorio:</strong> {reportData.directorio}</p>
                            </div>
                            <div className="col-md-4">
                                <p className="mb-1"><strong>Total de Facturas:</strong> {reportData.numero_facturas}</p>
                                <p className="mb-1"><strong>Monto Total:</strong> Bs. {reportData.monto_total_calculado.toFixed(2)}</p>
                            </div>
                            <div className="col-md-4">
                                <p className="mb-1"><strong>Anticipo:</strong> Bs. {Number(anticipo || 0).toFixed(2)}</p>
                                {(() => {
                                    const diferencia = Number(anticipo || 0) - reportData.monto_total_calculado;
                                    if (diferencia > 0) {
                                        return <p className="fw-bold text-success mb-1"><strong>A Pagar:</strong> Bs. {diferencia.toFixed(2)}</p>;
                                    } else if (diferencia < 0) {
                                        return <p className="fw-bold text-danger mb-1"><strong>A Cobrar:</strong> Bs. {(-diferencia).toFixed(2)}</p>;
                                    } else {
                                        return <p className="fw-bold mb-1"><strong>Diferencia:</strong> Bs. 0.00</p>;
                                    }
                                })()}
                            </div>
                        </div>
                        
                        <h6 className="mb-3">Facturas Incluidas:</h6>
                        {reportData.facturas && reportData.facturas.length > 0 ? (
                            <div className="table-responsive">
                                <table className="table table-striped table-hover">
                                    <thead className="table-light">
                                        <tr>
                                            <th>ID Factura</th>
                                            <th>Monto (Bs.)</th>
                                            <th>Fecha</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {reportData.facturas.map(factura => (
                                            <tr key={factura.id}>
                                                <td>{factura.id}</td>
                                                <td>{factura.montoTotal.toFixed(2)}</td>
                                                <td>{factura.fechaTransaccion}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        ) : (
                            <p>No se encontraron facturas para este periodo.</p>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
};

export default DashboardPage;