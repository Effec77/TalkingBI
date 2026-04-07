import Plot from "react-plotly.js";

const DESIGN_TOKENS = {
    background: '#12121d',
    onBackground: '#e3e0f1',
    primary: '#a0caff',
    primaryContainer: '#4f94dd',
    secondary: '#cdbdff',
    secondaryContainer: '#5203d5',
    tertiary: '#fbbb45',
    grid: 'rgba(65, 71, 81, 0.1)'
};

const DEFAULT_LAYOUT = {
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    font: {
        family: 'Inter, sans-serif',
        color: DESIGN_TOKENS.onBackground,
        size: 11
    },
    autosize: true,
    margin: { l: 40, r: 20, t: 40, b: 40 },
    xaxis: {
        gridcolor: DESIGN_TOKENS.grid,
        zerolinecolor: DESIGN_TOKENS.grid,
        tickfont: { color: 'rgba(227, 224, 241, 0.5)' }
    },
    yaxis: {
        gridcolor: DESIGN_TOKENS.grid,
        zerolinecolor: DESIGN_TOKENS.grid,
        tickfont: { color: 'rgba(227, 224, 241, 0.5)' }
    }
};

export default function ChartRenderer({ chart }: { chart: any }) {
    if (!chart) return null;

    let data: any[] = [];
    let layout: any = { ...DEFAULT_LAYOUT };

    if (chart.spec) {
        data = chart.spec.data.map((trace: any) => ({
            ...trace,
            marker: { ...trace.marker, color: trace.marker?.color || DESIGN_TOKENS.primary },
            line: { ...trace.line, color: trace.line?.color || DESIGN_TOKENS.primary }
        }));
        layout = { ...DEFAULT_LAYOUT, ...chart.spec.layout };
    } else if (chart.image) {
        return <img src={chart.image} alt="Oracle Visualization" className="rounded-2xl border border-white/5 shadow-2xl" />;
    } else {
        const baseTrace = {
            x: chart.x || chart.values,
            y: chart.y,
            marker: { color: DESIGN_TOKENS.primary },
            line: { color: DESIGN_TOKENS.primary, width: 2 }
        };

        switch (chart.type) {
            case "bar":
                data = [{ ...baseTrace, type: "bar" }];
                break;
            case "line":
                data = [{ ...baseTrace, type: "scatter", mode: "lines+markers", marker: { ...baseTrace.marker, size: 6 } }];
                break;
            case "histogram":
                data = [{ ...baseTrace, type: "histogram" }];
                break;
            default:
                return <div className="text-[10px] text-[#ffb4ab]">Oracle could not visualize this segment</div>;
        }
    }

    return (
        <div className="w-full h-full min-h-[300px]">
             <Plot
                data={data}
                layout={layout}
                useResizeHandler
                style={{ width: '100%', height: '100%' }}
                config={{ displayModeBar: false, responsive: true }}
            />
        </div>
    );
}
