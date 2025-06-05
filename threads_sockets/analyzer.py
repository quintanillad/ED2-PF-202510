import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.linear_model import LinearRegression
import os
import json
from statsmodels.tsa.stattools import adfuller
import warnings
warnings.filterwarnings('ignore')

# Configuración de estilos CORREGIDA
try:
    plt.style.use('seaborn-v0_8')  # Para versiones recientes de matplotlib
except:
    plt.style.use('ggplot')  # Estilo alternativo si el anterior no está disponible

sns.set_theme(style="whitegrid")  # Configuración moderna de seaborn
plt.rcParams['figure.figsize'] = (12, 6)

class SortingBenchmarkAnalyzer:
    def __init__(self, results_path='exports/resultados_completos.csv', detailed_dir='exports/detailed_results'):
        self.results_path = results_path
        self.detailed_dir = detailed_dir
        self.results_df = None
        self.iteration_data = None
        self.output_dir = 'analysis_results'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def load_data(self):
        """Carga los datos generados por el benchmark"""
        try:
            self.results_df = pd.read_csv(self.results_path)
            
            # Cargar datos de iteraciones
            self.iteration_data = {}
            if os.path.exists(self.detailed_dir):
                for file in os.listdir(self.detailed_dir):
                    if file.startswith('times_') and file.endswith('.json'):
                        method = file[6:-5]
                        with open(os.path.join(self.detailed_dir, file)) as f:
                            self.iteration_data[method] = json.load(f)
            else:
                print(f"Advertencia: No se encontró el directorio {self.detailed_dir}")
                
        except Exception as e:
            print(f"Error al cargar datos: {str(e)}")
            raise
    
    def basic_statistical_analysis(self):
        """Análisis estadístico descriptivo básico"""
        stats = self.results_df.groupby('method').agg({
            'avg_sort': ['mean', 'std', 'min', 'max', 'median'],
            'export_time': ['mean', 'std'],
            'file_size': ['mean', 'std']
        })
        
        # Guardar resultados
        stats.to_csv(os.path.join(self.output_dir, 'basic_stats.csv'))
        print(stats)
        
        return stats
    
    def performance_comparison(self):
        """Comparación visual del rendimiento"""
        plt.figure(figsize=(14, 7))
        
        # Boxplot de tiempos por algoritmo
        plt.subplot(1, 2, 1)
        sns.boxplot(x='method', y='avg_sort', data=self.results_df)
        plt.title('Distribución de Tiempos de Ordenación')
        plt.ylabel('Tiempo (s)')
        plt.xlabel('Algoritmo')
        
        # Barras de tiempo promedio por formato
        plt.subplot(1, 2, 2)
        sns.barplot(x='method', y='export_time', hue='format', data=self.results_df)
        plt.title('Tiempo Promedio de Exportación')
        plt.ylabel('Tiempo (s)')
        plt.xlabel('Algoritmo')
        plt.legend(title='Formato')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'performance_comparison.png'))
        plt.close()
    
    def statistical_tests(self):
        """Pruebas estadísticas para diferencias significativas"""
        methods = self.results_df['method'].unique()
        test_results = []
        
        # ANOVA y pruebas post-hoc
        anova_result = stats.f_oneway(*[self.results_df[self.results_df['method']==m]['avg_sort'] for m in methods])
        
        # Test de Tukey para comparaciones múltiples
        from statsmodels.stats.multicomp import pairwise_tukeyhsd
        tukey = pairwise_tukeyhsd(
            endog=self.results_df['avg_sort'],
            groups=self.results_df['method'],
            alpha=0.05
        )
        
        # Guardar resultados
        with open(os.path.join(self.output_dir, 'statistical_tests.txt'), 'w') as f:
            f.write(f"ANOVA Result:\n{anova_result}\n\n")
            f.write("Tukey HSD Test:\n")
            f.write(str(tukey.summary()))
        
        print("ANOVA Result:", anova_result)
        print("\nTukey HSD Test:")
        print(tukey.summary())
        
        return anova_result, tukey
    
    def temporal_analysis(self):
        """Análisis de la variación temporal en las iteraciones"""
        if not self.iteration_data:
            print("No hay datos de iteraciones para analizar")
            return
            
        plt.figure(figsize=(15, 10))
        
        for i, (method, data) in enumerate(self.iteration_data.items(), 1):
            plt.subplot(2, 2, i)
            plt.plot(data['iteration_times'], marker='o')
            
            # Test de estacionariedad
            adf_result = adfuller(data['iteration_times'])
            
            plt.title(f'{method} sort\nADF p-value: {adf_result[1]:.4f}')
            plt.xlabel('Iteración')
            plt.ylabel('Tiempo (s)')
            plt.grid(True)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'temporal_analysis.png'))
        plt.close()
    
    def scalability_analysis(self):
        """Análisis empírico de la complejidad computacional"""
        plt.figure(figsize=(12, 6))
        
        for method in self.results_df['method'].unique():
            subset = self.results_df[self.results_df['method']==method]
            sns.regplot(x='file_size', y='export_time', data=subset, 
                        label=method, scatter_kws={'alpha':0.6})
        
        plt.title('Relación Tamaño de Archivo vs Tiempo Exportación')
        plt.xlabel('Tamaño de Archivo (bytes)')
        plt.ylabel('Tiempo Exportación (s)')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(self.output_dir, 'scalability_analysis.png'))
        plt.close()
    
    def generate_comprehensive_report(self):
        """Genera un reporte completo en HTML"""
        from jinja2 import Environment, FileSystemLoader
        import datetime
        
        # Ruta completa al directorio que contiene el template
        template_dir = r'C:\Users\Sebastian Gonzalez\OneDrive\Documentos\Visual Studio Code\ED2-PF-202510\threads_sockets'
        
        # Preparar datos para el reporte
        context = {
            'date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'basic_stats': self.basic_statistical_analysis().to_html(),
            'performance_plots': {
                'comparison': 'performance_comparison.png',
                'temporal': 'temporal_analysis.png',
                'scalability': 'scalability_analysis.png'
            },
            'methods': self.results_df['method'].unique(),
            'formats': self.results_df['format'].unique()
        }
        
        try:
            # Renderizar plantilla HTML
            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template('report_template.html')
            html_report = template.render(context)
            
            # Guardar reporte
            with open(os.path.join(self.output_dir, 'benchmark_report.html'), 'w') as f:
                f.write(html_report)
            
            print(f"Reporte generado en: {os.path.abspath(os.path.join(self.output_dir, 'benchmark_report.html'))}")
        except Exception as e:
            print(f"Error al generar el reporte: {str(e)}")
            raise

def main():
    try:
        analyzer = SortingBenchmarkAnalyzer()
        analyzer.load_data()
        
        if analyzer.results_df is None:
            print("No se pudieron cargar los datos para análisis")
            return
        
        print("=== Análisis Estadístico Básico ===")
        analyzer.basic_statistical_analysis()
        
        print("\n=== Comparación de Rendimiento ===")
        analyzer.performance_comparison()
        
        print("\n=== Pruebas Estadísticas ===")
        analyzer.statistical_tests()
        
        print("\n=== Análisis Temporal ===")
        analyzer.temporal_analysis()
        
        print("\n=== Análisis de Escalabilidad ===")
        analyzer.scalability_analysis()
        
        print("\n=== Generando Reporte Completo ===")
        analyzer.generate_comprehensive_report()
        
    except Exception as e:
        print(f"Error durante el análisis: {str(e)}")

if __name__ == "__main__":
    main()