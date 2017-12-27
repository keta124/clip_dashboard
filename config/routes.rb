Rails.application.routes.draw do
  get 'products/show'

  get 'greetings/hello'

  resources :users
  resources :greetings
  # For details on the DSL available within this file, see http://guides.rubyonrails.org/routing.html
end
