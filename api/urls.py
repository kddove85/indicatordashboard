from django.urls import path
from . import views


urlpatterns = [
    path('gdp/', views.get_alpha_gdp),
    path('cpi/', views.get_cpi),
    path('unemployment/', views.get_unemployment),
    path('immigration/', views.get_immigration),
    path('deportation/', views.get_deportation),
    path('spending/', views.get_department_spending),
    path('gas/', views.get_monthly_gas_prices),
    path('orders/', views.get_executive_orders),
    path('deficit/', views.get_deficit),
    path('initial_approval/', views.get_initial_approval),
    path('final_approval/', views.get_final_approval),
]
