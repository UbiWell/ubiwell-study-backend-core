from abc import ABC, abstractmethod
from typing import List, Dict, Any


class LandingPageBase(ABC):
    """
    Base class for Landing Page functionality.
    
    This class defines the interface that all study-specific Landing Pages must implement.
    Study-specific Landing Pages can extend this class to add custom modules and functionality.
    """

    def __init__(self):
        self._core_modules = self._get_core_modules()
        self._custom_modules = self._get_custom_modules()
        
        # Cache for performance
        self._all_modules_cache = None
    
    def _get_core_modules(self) -> List[Dict[str, Any]]:
        """Get list of core modules available to all studies."""
        return [
            {
                'id': 'dashboard',
                'name': 'Dashboard',
                'description': 'View study compliance and data monitoring dashboard',
                'icon': 'fas fa-chart-line',
                'url': '/internal_web/dashboard',
                'color': 'primary'
            },
            {
                'id': 'user_management',
                'name': 'Participant Management',
                'description': 'Create and manage study participants',
                'icon': 'fas fa-users',
                'url': '/internal_web/user-management',
                'color': 'success'
            },
            {
                'id': 'ema_schedule',
                'name': 'EMA Schedule',
                'description': 'Manage EMA surveys and schedules',
                'icon': 'fas fa-calendar-alt',
                'url': '/internal_web/ema-schedule',
                'color': 'info'
            },
            {
                'id': 'config_schedule',
                'name': 'Configuration',
                'description': 'Manage study configuration and settings',
                'icon': 'fas fa-cogs',
                'url': '/internal_web/config-schedule',
                'color': 'warning'
            },
            {
                'id': 'downloads',
                'name': 'Data Downloads',
                'description': 'Download compliance reports and data exports',
                'icon': 'fas fa-download',
                'url': '/internal_web/download_compliance',
                'color': 'secondary'
            }
        ]
    
    @abstractmethod
    def _get_custom_modules(self) -> List[Dict[str, Any]]:
        """
        Get study-specific custom modules.
        
        Override this method in study-specific implementations to add custom modules.
        Each module should be a dictionary with the following structure:
        {
            'id': str,              # Unique identifier for the module
            'name': str,            # Display name
            'description': str,     # Short description
            'icon': str,            # Font Awesome icon class
            'url': str,             # URL path
            'color': str            # Bootstrap color class (primary, success, info, etc.)
        }
        
        Returns:
            List of custom module dictionaries
        """
        return []
    
    def get_all_modules(self) -> List[Dict[str, Any]]:
        """Get all modules (core + custom) with caching for performance."""
        if self._all_modules_cache is None:
            self._all_modules_cache = self._core_modules + self._custom_modules
        return self._all_modules_cache
    
    def clear_cache(self):
        """Clear the modules cache (useful for testing or dynamic updates)."""
        self._all_modules_cache = None
    
    def get_module_by_id(self, module_id: str) -> Dict[str, Any]:
        """Get a specific module by its ID."""
        for module in self.get_all_modules():
            if module['id'] == module_id:
                return module
        return None
    
    def get_template_context(self, username: str) -> Dict[str, Any]:
        """
        Get template context for rendering the landing page.
        
        Args:
            username: Current logged-in username
            
        Returns:
            Dictionary with template context
        """
        return {
            'modules': self.get_all_modules(),
            'username': username,
            'custom_css': self.get_custom_css(),
            'custom_content': self.get_custom_content(),
            'custom_scripts': self.get_custom_scripts()
        }
    
    # Extension points for study-specific customizations
    def get_custom_css(self) -> str:
        """
        Get custom CSS for study-specific styling.
        
        Override this method in study-specific implementations to add custom CSS.
        
        Returns:
            String containing custom CSS
        """
        return ""
    
    def get_custom_content(self) -> str:
        """
        Get custom HTML content to inject into the landing page.
        
        Override this method in study-specific implementations to add custom content.
        
        Returns:
            String containing custom HTML content
        """
        return ""
    
    def get_custom_scripts(self) -> str:
        """
        Get custom JavaScript for study-specific functionality.
        
        Override this method in study-specific implementations to add custom scripts.
        
        Returns:
            String containing custom JavaScript
        """
        return ""